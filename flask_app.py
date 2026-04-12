import os
import json
import csv
import io
from datetime import datetime
from urllib.parse import quote_plus

import requests
from flask import Flask, request, session, Response, send_file, jsonify, render_template
import anthropic

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Module-level storage for leads and outreach (per process)
_leads_store = []
_outreach_store = []


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "web_search",
        "description": (
            "Search the web for any information — news, business details, market research, "
            "contact info, pricing, reviews, or anything else. Use this whenever you need "
            "current information from the internet."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "apollo_search_people",
        "description": (
            "Search for companies/organizations on Apollo.io by keyword and location. "
            "Returns company name, website, phone, LinkedIn, address, and industry."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keywords":    {"type": "string", "description": "Industry keywords, e.g. 'nail salon'"},
                "locations":   {"type": "array",  "items": {"type": "string"},
                                "description": "Locations, e.g. ['Miami, FL']"},
                "num_results": {"type": "integer", "description": "Number of results (max 50)", "default": 20},
            },
            "required": ["keywords"],
        },
    },
    {
        "name": "hubspot_create_contact",
        "description": "Create a new contact in HubSpot CRM.",
        "input_schema": {
            "type": "object",
            "properties": {
                "email":      {"type": "string"},
                "first_name": {"type": "string"},
                "last_name":  {"type": "string"},
                "company":    {"type": "string"},
                "phone":      {"type": "string"},
                "website":    {"type": "string"},
                "job_title":  {"type": "string"},
                "linkedin":   {"type": "string"},
            },
            "required": ["email"],
        },
    },
    {
        "name": "save_outreach_csv",
        "description": "Save email drafts to a CSV file called outreach_drafts.csv.",
        "input_schema": {
            "type": "object",
            "properties": {
                "drafts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name":         {"type": "string"},
                            "email":        {"type": "string"},
                            "subject_line": {"type": "string"},
                            "email_body":   {"type": "string"},
                        },
                    },
                    "description": "List of email drafts",
                }
            },
            "required": ["drafts"],
        },
    },
]


# ── Tool implementations ───────────────────────────────────────────────────────

def web_search(query):
    try:
        url = f"https://api.duckduckgo.com/?q={quote_plus(query)}&format=json&no_html=1&skip_disambig=1"
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        results = []
        if data.get("AbstractText"):
            results.append(f"Summary: {data['AbstractText']}")
            if data.get("AbstractURL"):
                results.append(f"Source: {data['AbstractURL']}")
        for topic in data.get("RelatedTopics", [])[:5]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append(f"- {topic['Text']}")
                if topic.get("FirstURL"):
                    results.append(f"  {topic['FirstURL']}")
        if data.get("Answer"):
            results.append(f"Answer: {data['Answer']}")
        if not results:
            search_url = f"https://www.google.com/search?q={quote_plus(query)}"
            results.append(f"No direct results. Try searching: {search_url}")
        return {"results": "\n".join(results), "query": query}
    except Exception as e:
        return {"error": str(e)}


def apollo_search_people(keywords=None, locations=None, num_results=20, _apollo_key=None):
    global _leads_store
    apollo_key = _apollo_key or os.getenv("APOLLO_API_KEY", "")
    if not apollo_key:
        return {"error": "Apollo API key not configured. Please set it in Settings."}
    payload = {
        "page":                        1,
        "per_page":                    min(num_results or 20, 50),
        "q_organization_keyword_tags": [keywords] if keywords else [],
        "organization_locations":      locations or [],
    }
    try:
        r = requests.post(
            "https://api.apollo.io/v1/organizations/search",
            json=payload,
            headers={
                "Content-Type":  "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key":     apollo_key,
            },
            timeout=30,
        )
        data = r.json()
        if "organizations" not in data:
            return {"error": f"Apollo error: {data.get('error', data)}"}
        leads = []
        for org in data["organizations"]:
            phone = org.get("phone") or ""
            if not phone:
                pp = org.get("primary_phone") or {}
                phone = pp.get("sanitized_number") or pp.get("number") or ""
            lead = {
                "company":   org.get("name", ""),
                "website":   org.get("website_url", ""),
                "phone":     phone,
                "linkedin":  org.get("linkedin_url", ""),
                "industry":  org.get("industry", ""),
                "city":      org.get("city", ""),
                "state":     org.get("state", ""),
                "address":   org.get("raw_address", ""),
                "employees": org.get("estimated_num_employees", ""),
            }
            leads.append(lead)
        _leads_store = leads
        # Save to file
        _save_leads_to_file(leads)
        return {"leads": leads, "total": len(leads)}
    except Exception as e:
        return {"error": str(e)}


def _save_leads_to_file(leads):
    try:
        path = os.path.join(os.path.dirname(__file__), "leads.csv")
        fieldnames = ["company", "website", "phone", "linkedin", "industry", "city", "state", "address", "employees"]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(leads)
    except Exception:
        pass


def hubspot_create_contact(email, first_name="", last_name="", company="",
                            phone="", website="", job_title="", linkedin="", _hubspot_token=None):
    hubspot_token = _hubspot_token or os.getenv("HUBSPOT_TOKEN", "")
    if not hubspot_token:
        return {"error": "HubSpot token not configured. Please set it in Settings."}
    properties = {"email": email}
    if first_name: properties["firstname"]    = first_name
    if last_name:  properties["lastname"]     = last_name
    if company:    properties["company"]      = company
    if phone:      properties["phone"]        = phone
    if website:    properties["website"]      = website
    if job_title:  properties["jobtitle"]     = job_title
    if linkedin:   properties["linkedin_bio"] = linkedin
    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            json={"properties": properties},
            headers={
                "Authorization": f"Bearer {hubspot_token.strip()}",
                "Content-Type":  "application/json",
            },
            timeout=20,
        )
        data = r.json()
        if r.status_code in (200, 201):
            return {"success": True, "id": data.get("id"), "email": email, "company": company}
        elif r.status_code == 409:
            return {"success": False, "error": "Contact already exists", "email": email, "company": company}
        else:
            return {"success": False, "error": data.get("message", str(data)), "email": email, "company": company}
    except Exception as e:
        return {"error": str(e)}


def save_outreach_csv(drafts):
    global _outreach_store
    _outreach_store = drafts
    try:
        path = os.path.join(os.path.dirname(__file__), "outreach_drafts.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "email", "subject_line", "email_body"],
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(drafts)
        return {"success": True, "path": path, "count": len(drafts)}
    except Exception as e:
        return {"error": str(e)}


TOOL_MAP = {
    "web_search":             web_search,
    "apollo_search_people":   apollo_search_people,
    "hubspot_create_contact": hubspot_create_contact,
    "save_outreach_csv":      save_outreach_csv,
}


def run_tool(name, inputs, apollo_key="", hubspot_token=""):
    fn = TOOL_MAP.get(name)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    kwargs = dict(inputs)
    if name == "apollo_search_people":
        kwargs["_apollo_key"] = apollo_key
    elif name == "hubspot_create_contact":
        kwargs["_hubspot_token"] = hubspot_token
    return fn(**kwargs)


# ── Agentic loop (generator) ──────────────────────────────────────────────────

def run_agent(user_message: str, history: list, anthropic_key="", apollo_key="", hubspot_token=""):
    """
    Generator that yields SSE-formatted strings.
    Events: text, tool_start, tool_end, done, error
    """
    anthropic_key = anthropic_key or os.getenv("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        yield f"data: {json.dumps({'type': 'text', 'content': 'Please configure your Anthropic API key in Settings.'})}\n\n"
        yield f"data: {json.dumps({'type': 'done'})}\n\n"
        return

    client = anthropic.Anthropic(api_key=anthropic_key)

    system = (
        "You are MMG Agent, a lead generation assistant. You help users find prospects using Apollo, "
        "enrich their data, load them into HubSpot CRM, and draft outreach emails. "
        "Be concise and action-oriented. When asked to perform a step, do it immediately "
        "using the available tools. After each tool call, report what happened."
    )

    # Build API messages from history
    api_messages = []
    for msg in history:
        role = msg.get("role")
        content = msg.get("content")
        if role in ("user", "assistant") and content:
            api_messages.append({"role": role, "content": content})
    api_messages.append({"role": "user", "content": user_message})

    try:
        while True:
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=4096,
                system=system,
                tools=TOOLS,
                messages=api_messages,
            )

            full_text = ""
            tool_calls = []
            for block in response.content:
                if block.type == "text":
                    full_text += block.text
                elif block.type == "tool_use":
                    tool_calls.append(block)

            if full_text:
                yield f"data: {json.dumps({'type': 'text', 'content': full_text})}\n\n"

            if response.stop_reason == "end_turn" or not tool_calls:
                break

            api_messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tc in tool_calls:
                yield f"data: {json.dumps({'type': 'tool_start', 'name': tc.name})}\n\n"
                result = run_tool(tc.name, tc.input, apollo_key=apollo_key, hubspot_token=hubspot_token)
                yield f"data: {json.dumps({'type': 'tool_end', 'name': tc.name, 'result': result})}\n\n"
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": tc.id,
                    "content":     json.dumps(result),
                })

            api_messages.append({"role": "user", "content": tool_results})

    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    yield f"data: {json.dumps({'type': 'done'})}\n\n"


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({
        "anthropic": bool(session.get("anthropic_key") or os.getenv("ANTHROPIC_API_KEY")),
        "apollo":    bool(session.get("apollo_key")    or os.getenv("APOLLO_API_KEY")),
        "hubspot":   bool(session.get("hubspot_token") or os.getenv("HUBSPOT_TOKEN")),
    })


@app.route("/api/config", methods=["POST"])
def save_config():
    data = request.get_json(force=True)
    if data.get("anthropic_key"):
        session["anthropic_key"] = data["anthropic_key"]
    if data.get("apollo_key"):
        session["apollo_key"] = data["apollo_key"]
    if data.get("hubspot_token"):
        session["hubspot_token"] = data["hubspot_token"]
    return jsonify({"ok": True})


@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)
    message = data.get("message", "")
    history = data.get("history", [])

    # Read session values NOW, before entering the streaming generator
    # (Flask session is not available inside a Response generator)
    anthropic_key  = session.get("anthropic_key")  or os.getenv("ANTHROPIC_API_KEY", "")
    apollo_key     = session.get("apollo_key")     or os.getenv("APOLLO_API_KEY", "")
    hubspot_token  = session.get("hubspot_token")  or os.getenv("HUBSPOT_TOKEN", "")

    def stream():
        yield from run_agent(message, history,
                             anthropic_key=anthropic_key,
                             apollo_key=apollo_key,
                             hubspot_token=hubspot_token)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/download/leads")
def download_leads():
    path = os.path.join(os.path.dirname(__file__), "leads.csv")
    if os.path.exists(path):
        return send_file(path, mimetype="text/csv",
                         as_attachment=True, download_name="leads.csv")
    # Build from memory
    global _leads_store
    if not _leads_store:
        return jsonify({"error": "No leads available"}), 404
    output = io.StringIO()
    fieldnames = ["company", "website", "phone", "linkedin", "industry", "city", "state", "address", "employees"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(_leads_store)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@app.route("/api/download/outreach")
def download_outreach():
    path = os.path.join(os.path.dirname(__file__), "outreach_drafts.csv")
    if os.path.exists(path):
        return send_file(path, mimetype="text/csv",
                         as_attachment=True, download_name="outreach_drafts.csv")
    global _outreach_store
    if not _outreach_store:
        return jsonify({"error": "No outreach drafts available"}), 404
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "email", "subject_line", "email_body"],
                            extrasaction="ignore")
    writer.writeheader()
    writer.writerows(_outreach_store)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=outreach_drafts.csv"},
    )


if __name__ == "__main__":
    app.run(port=8501, debug=False)
