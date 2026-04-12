import streamlit as st
import anthropic
import requests
import json
import csv
import io
import os
from datetime import datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lead Gen Agent",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Lead Gen Agent")
st.caption("Powered by Claude · Apollo · HubSpot")

# ── Sidebar: API keys ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuration")
    anthropic_key = st.text_input("Anthropic API Key", type="password",
                                   value=os.getenv("ANTHROPIC_API_KEY", ""))
    apollo_key    = st.text_input("Apollo API Key",    type="password",
                                   value=os.getenv("APOLLO_API_KEY", ""))
    hubspot_token = st.text_input("HubSpot Token",     type="password",
                                   value=os.getenv("HUBSPOT_TOKEN", ""))
    st.divider()
    if st.button("🔌 Test HubSpot connection"):
        if not hubspot_token:
            st.error("No token entered.")
        else:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/contacts?limit=1",
                headers={"Authorization": f"Bearer {hubspot_token.strip()}"},
            )
            if r.status_code == 200:
                st.success("✅ HubSpot connected!")
            else:
                st.error(f"❌ {r.status_code}: {r.json().get('message', r.text)}")
    st.divider()
    st.markdown("**Example prompts:**")
    st.markdown("- Find 10 nail salon owners in Miami")
    st.markdown("- Load leads to HubSpot")
    st.markdown("- Draft outreach emails for all leads")
    st.markdown("- Save drafts to outreach_drafts.csv")
    if st.button("🗑️ Clear chat"):
        st.session_state.messages = []
        st.session_state.leads = []
        st.rerun()

# ── Session state ─────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "leads" not in st.session_state:
    st.session_state.leads = []

# ── Tool definitions ──────────────────────────────────────────────────────────
TOOLS = [
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
        "name": "apollo_enrich_person",
        "description": "Enrich a person's contact info (fill in missing email/phone) using Apollo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "first_name": {"type": "string"},
                "last_name":  {"type": "string"},
                "company":    {"type": "string"},
                "domain":     {"type": "string", "description": "Company website domain"},
            },
            "required": ["first_name", "last_name"],
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

def apollo_search_people(titles=None, locations=None, keywords=None, num_results=20):
    if not apollo_key:
        return {"error": "Apollo API key not configured."}

    payload = {
        "page":                          1,
        "per_page":                      min(num_results, 50),
        "q_organization_keyword_tags":   [keywords] if keywords else [],
        "organization_locations":        locations or [],
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
                "company":  org.get("name", ""),
                "website":  org.get("website_url", ""),
                "phone":    phone,
                "linkedin": org.get("linkedin_url", ""),
                "industry": org.get("industry", ""),
                "city":     org.get("city", ""),
                "state":    org.get("state", ""),
                "address":  org.get("raw_address", ""),
                "employees": org.get("estimated_num_employees", ""),
            }
            leads.append(lead)

        st.session_state.leads = leads
        return {"leads": leads, "total": len(leads)}

    except Exception as e:
        return {"error": str(e)}


def apollo_enrich_person(first_name, last_name, company=None, domain=None):
    if not apollo_key:
        return {"error": "Apollo API key not configured."}

    payload = {
        "api_key":           apollo_key,
        "first_name":        first_name,
        "last_name":         last_name,
        "organization_name": company or "",
        "domain":            domain or "",
    }

    try:
        r = requests.post(
            "https://api.apollo.io/v1/people/match",
            json=payload,
            headers={
                "Content-Type":  "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key":     apollo_key,
            },
            timeout=20,
        )
        data = r.json()
        person = data.get("person", {})
        return {
            "email": person.get("email", ""),
            "phone": person.get("phone",  ""),
        }
    except Exception as e:
        return {"error": str(e)}


def hubspot_create_contact(email, first_name="", last_name="", company="",
                            phone="", website="", job_title="", linkedin=""):
    if not hubspot_token:
        return {"error": "HubSpot token not configured."}

    properties = {"email": email}
    if first_name: properties["firstname"]   = first_name
    if last_name:  properties["lastname"]    = last_name
    if company:    properties["company"]     = company
    if phone:      properties["phone"]       = phone
    if website:    properties["website"]     = website
    if job_title:  properties["jobtitle"]    = job_title
    if linkedin:   properties["linkedin_bio"] = linkedin

    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            json={"properties": properties},
            headers={
                "Authorization": f"Bearer {hubspot_token.strip()}",
                "Content-Type": "application/json",
            },
            timeout=20,
        )
        data = r.json()
        if r.status_code in (200, 201):
            return {"success": True, "id": data.get("id"), "email": email}
        elif r.status_code == 409:
            return {"success": False, "error": "Contact already exists", "email": email}
        else:
            return {"success": False, "error": data.get("message", str(data)), "email": email}
    except Exception as e:
        return {"error": str(e)}


def save_leads_csv():
    """Save the current leads list to leads.csv."""
    if not st.session_state.leads:
        return {"error": "No leads to save."}
    output = io.StringIO()
    fieldnames = ["company", "website", "phone", "linkedin", "industry", "city", "state", "address", "employees"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(st.session_state.leads)
    csv_content = output.getvalue()
    path = os.path.join(os.path.dirname(__file__), "leads.csv")
    with open(path, "w", newline="") as f:
        f.write(csv_content)
    st.session_state["leads_csv_download"] = csv_content
    return {"success": True, "path": path, "count": len(st.session_state.leads)}


def save_outreach_csv(drafts):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "email", "subject_line", "email_body"])
    writer.writeheader()
    writer.writerows(drafts)
    csv_content = output.getvalue()

    # Save to disk
    path = os.path.join(os.path.dirname(__file__), "outreach_drafts.csv")
    with open(path, "w", newline="") as f:
        f.write(csv_content)

    # Also offer download in the UI
    st.session_state["csv_download"] = csv_content
    return {"success": True, "path": path, "count": len(drafts)}


TOOL_MAP = {
    "apollo_search_people":   apollo_search_people,
    "apollo_enrich_person":   apollo_enrich_person,
    "hubspot_create_contact": hubspot_create_contact,
    "save_outreach_csv":      save_outreach_csv,
    "save_leads_csv":         lambda: save_leads_csv(),
}


def run_tool(name, inputs):
    fn = TOOL_MAP.get(name)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    return fn(**inputs)


# ── Agentic loop ───────────────────────────────────────────────────────────────

def stream_agent_response(user_message: str):
    """Run Claude with tool use and yield text chunks + tool events."""
    if not anthropic_key:
        yield "text", "⚠️ Please enter your Anthropic API key in the sidebar."
        return

    client = anthropic.Anthropic(api_key=anthropic_key)

    system = (
        "You are a lead generation assistant. You help users find prospects using Apollo, "
        "enrich their data, load them into HubSpot CRM, and draft outreach emails. "
        "Be concise and action-oriented. When asked to perform a step, do it immediately "
        "using the available tools. After each tool call, report what happened."
    )

    # Build message history
    api_messages = []
    for msg in st.session_state.messages:
        api_messages.append({"role": msg["role"], "content": msg["content"]})
    api_messages.append({"role": "user", "content": user_message})

    while True:
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            system=system,
            tools=TOOLS,
            messages=api_messages,
        )

        # Collect text
        full_text = ""
        tool_calls = []
        for block in response.content:
            if block.type == "text":
                full_text += block.text
            elif block.type == "tool_use":
                tool_calls.append(block)

        if full_text:
            yield "text", full_text

        # Done?
        if response.stop_reason == "end_turn" or not tool_calls:
            break

        # Append assistant turn
        api_messages.append({"role": "assistant", "content": response.content})

        # Execute tools
        tool_results = []
        for tc in tool_calls:
            yield "tool_start", tc.name

            result = run_tool(tc.name, tc.input)
            yield "tool_end", (tc.name, result)

            tool_results.append({
                "type":        "tool_result",
                "tool_use_id": tc.id,
                "content":     json.dumps(result),
            })

        api_messages.append({"role": "user", "content": tool_results})


# ── Chat history display ───────────────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ── CSV download buttons ───────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    if st.session_state.get("leads_csv_download"):
        st.download_button("⬇️ Download leads.csv", data=st.session_state["leads_csv_download"],
                           file_name="leads.csv", mime="text/csv")
with col2:
    if st.session_state.get("csv_download"):
        st.download_button("⬇️ Download outreach_drafts.csv", data=st.session_state["csv_download"],
                           file_name="outreach_drafts.csv", mime="text/csv")

# ── Chat input ────────────────────────────────────────────────────────────────
if prompt := st.chat_input("Ask me to find leads, load to HubSpot, draft emails…"):

    # Show user message
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Stream assistant response
    with st.chat_message("assistant"):
        response_placeholder = st.empty()
        full_response = ""

        for event_type, payload in stream_agent_response(prompt):
            if event_type == "text":
                full_response += payload
                response_placeholder.markdown(full_response + "▌")

            elif event_type == "tool_start":
                tool_icons = {
                    "apollo_search_people":   "🔍",
                    "apollo_enrich_person":   "🔎",
                    "hubspot_create_contact": "📥",
                    "save_outreach_csv":      "💾",
                }
                icon = tool_icons.get(payload, "⚙️")
                full_response += f"\n\n{icon} *Running `{payload}`…*\n\n"
                response_placeholder.markdown(full_response + "▌")

            elif event_type == "tool_end":
                tool_name, result = payload
                # Show a brief summary
                if tool_name == "apollo_search_people" and "leads" in result:
                    full_response += f"Found **{result['total']} leads**.\n\n"
                elif tool_name == "hubspot_create_contact":
                    if result.get("success"):
                        full_response += f"✅ Contact created ({result.get('email')})\n\n"
                    else:
                        full_response += f"⚠️ {result.get('error', 'Error')} ({result.get('email')})\n\n"
                elif tool_name == "save_outreach_csv" and result.get("success"):
                    full_response += f"💾 Saved **{result['count']} drafts** to `{result['path']}`\n\n"
                response_placeholder.markdown(full_response + "▌")

        response_placeholder.markdown(full_response)

    st.session_state.messages.append({"role": "assistant", "content": full_response})

    # Refresh to show download button if CSV was saved
    if st.session_state.get("csv_download"):
        st.rerun()
