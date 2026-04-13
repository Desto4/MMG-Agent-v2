import streamlit as st
import anthropic
import requests
import json
import csv
import io
import os
from datetime import datetime
from urllib.parse import quote_plus

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MMG Agent",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Reset & base ── */
[data-testid="stAppViewContainer"] {
    background: #F4F6F9;
}
[data-testid="stSidebar"] {
    background: #0B1120 !important;
    border-right: 1px solid #151D2E;
}
[data-testid="stSidebar"] > div:first-child {
    padding-top: 0 !important;
}

/* Hide default Streamlit chrome */
#MainMenu, footer, header {visibility: hidden;}
[data-testid="stDecoration"] {display: none;}

/* ── Sidebar typography ── */
.sidebar-logo {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 22px 20px 18px 20px;
    border-bottom: 1px solid #151D2E;
    margin-bottom: 8px;
}
.sidebar-logo-icon {
    width: 34px;
    height: 34px;
    background: #00C67F;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    font-weight: 800;
    color: white;
    flex-shrink: 0;
}
.sidebar-logo-text {
    font-size: 16px;
    font-weight: 700;
    color: #FFFFFF;
    letter-spacing: 0.3px;
}

/* ── New task button in sidebar ── */
.sidebar-new-task-btn {
    margin: 4px 16px 16px 16px;
}
.sidebar-new-task-btn button {
    width: 100%;
    background: #00C67F !important;
    color: #fff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 14px !important;
    padding: 10px 0 !important;
    cursor: pointer;
}
.sidebar-new-task-btn button:hover {
    background: #00A86B !important;
}

/* ── Sidebar nav label ── */
.sidebar-nav-label {
    font-size: 11px;
    font-weight: 600;
    color: #3D4F63;
    letter-spacing: 1px;
    text-transform: uppercase;
    padding: 4px 20px 8px 20px;
}

/* ── Nav items ── */
.nav-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 9px 20px;
    margin: 2px 8px;
    border-radius: 8px;
    cursor: pointer;
    color: #7A8FA6;
    font-size: 14px;
    font-weight: 500;
    transition: all 0.15s;
    text-decoration: none;
}
.nav-item:hover {
    background: #162032;
    color: #FFFFFF;
}
.nav-item.active {
    background: #162032;
    color: #FFFFFF;
}
.nav-item-icon {
    font-size: 16px;
    width: 20px;
    text-align: center;
}

/* ── Sidebar user profile ── */
.sidebar-user {
    position: fixed;
    bottom: 0;
    width: 260px;
    border-top: 1px solid #151D2E;
    padding: 14px 20px;
    background: #0B1120;
    display: flex;
    align-items: center;
    gap: 12px;
}
.user-avatar {
    width: 34px;
    height: 34px;
    background: linear-gradient(135deg, #4A9EFF 0%, #6B5AED 100%);
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    font-weight: 700;
    color: white;
    flex-shrink: 0;
}
.user-info-name {
    font-size: 13px;
    font-weight: 600;
    color: #FFFFFF;
}
.user-info-role {
    font-size: 11px;
    color: #4A5568;
}

/* ── Main content header ── */
.page-header {
    padding: 28px 32px 16px 32px;
    border-bottom: 1px solid #E8ECF0;
    background: #FFFFFF;
    margin-bottom: 0;
}
.page-header-row {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
}
.page-title {
    font-size: 22px;
    font-weight: 700;
    color: #0F1623;
    margin: 0 0 4px 0;
}
.page-subtitle {
    font-size: 13px;
    color: #6B7280;
    margin: 0;
}

/* ── Buttons ── */
.btn-primary {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #00C67F;
    color: white;
    border: none;
    border-radius: 8px;
    padding: 9px 18px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    transition: background 0.15s;
}
.btn-primary:hover {
    background: #00A86B;
}
.btn-secondary {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #F3F4F6;
    color: #374151;
    border: 1px solid #E5E7EB;
    border-radius: 8px;
    padding: 8px 16px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    text-decoration: none;
    transition: all 0.15s;
}
.btn-secondary:hover {
    background: #E5E7EB;
}

/* ── Table styles ── */
.tasks-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
}
.tasks-table th {
    font-size: 11px;
    font-weight: 600;
    color: #6B7280;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    padding: 12px 20px;
    border-bottom: 1px solid #E8ECF0;
    text-align: left;
}
.tasks-table td {
    padding: 14px 20px;
    border-bottom: 1px solid #F3F4F6;
    font-size: 14px;
    color: #111827;
    vertical-align: middle;
}
.tasks-table tr:hover td {
    background: #F9FAFB;
    cursor: pointer;
}
.tasks-table tr:last-child td {
    border-bottom: none;
}

/* ── Status badges ── */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
}
.badge-green {
    background: #DCFCE7;
    color: #15803D;
}
.badge-blue {
    background: #DBEAFE;
    color: #1D4ED8;
}
.badge-red {
    background: #FEE2E2;
    color: #B91C1C;
}
.badge-gray {
    background: #F1F5F9;
    color: #475569;
}

/* ── Output pills ── */
.output-pill {
    display: inline-flex;
    align-items: center;
    background: #F3F4F6;
    color: #374151;
    border-radius: 6px;
    padding: 3px 9px;
    font-size: 12px;
    font-weight: 500;
    margin-right: 5px;
    margin-bottom: 3px;
}
.output-pill-blue {
    background: #EFF6FF;
    color: #1D4ED8;
}
.output-pill-green {
    background: #ECFDF5;
    color: #065F46;
}

/* ── Chat / Agent Session ── */
.chat-container {
    padding: 24px 32px;
    max-width: 900px;
}
.chat-bubble-user {
    display: flex;
    justify-content: flex-end;
    margin-bottom: 20px;
}
.chat-bubble-user-inner {
    background: #0F1623;
    color: white;
    border-radius: 16px 16px 4px 16px;
    padding: 12px 18px;
    max-width: 600px;
    font-size: 14px;
    line-height: 1.5;
}
.chat-bubble-agent {
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
}
.agent-avatar {
    width: 34px;
    height: 34px;
    background: #00C67F;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    flex-shrink: 0;
    margin-top: 2px;
}
.agent-message-wrapper {
    flex: 1;
}
.agent-label {
    font-size: 12px;
    font-weight: 600;
    color: #6B7280;
    margin-bottom: 6px;
}
.agent-message-inner {
    background: white;
    border: 1px solid #E8ECF0;
    border-radius: 4px 16px 16px 16px;
    padding: 14px 18px;
    font-size: 14px;
    line-height: 1.6;
    color: #111827;
}

/* ── Apollo results table ── */
.apollo-table {
    width: 100%;
    border-collapse: collapse;
    margin: 12px 0;
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid #E8ECF0;
}
.apollo-table th {
    font-size: 11px;
    font-weight: 600;
    color: #6B7280;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    padding: 10px 14px;
    background: #F9FAFB;
    text-align: left;
    border-bottom: 1px solid #E8ECF0;
}
.apollo-table td {
    padding: 10px 14px;
    border-bottom: 1px solid #F3F4F6;
    font-size: 13px;
    color: #111827;
    vertical-align: middle;
}
.apollo-table tr:last-child td {
    border-bottom: none;
}
.score-pill {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #DBEAFE;
    color: #1E40AF;
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 12px;
    font-weight: 700;
    min-width: 36px;
}

/* ── HubSpot sync list ── */
.sync-list {
    margin: 10px 0;
}
.sync-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 7px 0;
    font-size: 13px;
    color: #374151;
    border-bottom: 1px solid #F3F4F6;
}
.sync-item:last-child {
    border-bottom: none;
}
.sync-check {
    color: #00C67F;
    font-size: 15px;
}
.sync-arrow {
    color: #9CA3AF;
    font-size: 12px;
}

/* ── Action buttons in chat ── */
.action-buttons {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 14px;
}
.action-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: #F9FAFB;
    color: #374151;
    border: 1px solid #E5E7EB;
    border-radius: 8px;
    padding: 7px 14px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s;
    text-decoration: none;
}
.action-btn:hover {
    background: #F3F4F6;
    border-color: #D1D5DB;
}

/* ── Connectors ── */
.section-label {
    font-size: 11px;
    font-weight: 700;
    color: #9CA3AF;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    padding: 24px 32px 12px 32px;
}
.connector-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 16px;
    padding: 0 32px 32px 32px;
}
.connector-card {
    background: white;
    border: 1px solid #E8ECF0;
    border-radius: 12px;
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 12px;
}
.connector-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.connector-identity {
    display: flex;
    align-items: center;
    gap: 12px;
}
.connector-icon {
    width: 40px;
    height: 40px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 22px;
    background: #F3F4F6;
    flex-shrink: 0;
}
.connector-name {
    font-size: 15px;
    font-weight: 600;
    color: #0F1623;
}
.connector-desc {
    font-size: 13px;
    color: #6B7280;
    line-height: 1.4;
}
.connector-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 4px;
}
.connector-meta {
    font-size: 12px;
    color: #9CA3AF;
}

/* ── Tool status widget ── */
.tool-running {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: #EFF6FF;
    color: #1D4ED8;
    border-radius: 8px;
    padding: 6px 12px;
    font-size: 13px;
    font-weight: 500;
    margin: 6px 0;
}
.tool-done {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: #ECFDF5;
    color: #065F46;
    border-radius: 8px;
    padding: 6px 12px;
    font-size: 13px;
    font-weight: 500;
    margin: 6px 0;
}

/* ── Settings expander ── */
.settings-section {
    margin: 8px;
}

/* ── Empty state ── */
.empty-state {
    text-align: center;
    padding: 80px 40px;
}
.empty-state-icon {
    font-size: 48px;
    margin-bottom: 16px;
}
.empty-state-title {
    font-size: 18px;
    font-weight: 600;
    color: #374151;
    margin-bottom: 8px;
}
.empty-state-desc {
    font-size: 14px;
    color: #9CA3AF;
    margin-bottom: 24px;
}

/* ── Streamlit widget overrides ── */
[data-testid="stTextInput"] input {
    background: #1A2535 !important;
    border: 1px solid #1E2A3A !important;
    color: #E2E8F0 !important;
    border-radius: 8px !important;
}
[data-testid="stTextInput"] label {
    color: #8899AA !important;
    font-size: 12px !important;
}
div[data-testid="stExpander"] {
    background: transparent !important;
    border: 1px solid #1E2A3A !important;
    border-radius: 8px !important;
}
div[data-testid="stExpander"] summary {
    color: #8899AA !important;
    font-size: 13px !important;
}

/* ── Chat input override ── */
[data-testid="stChatInput"] textarea {
    background: #FFFFFF !important;
    border: 1px solid #E5E7EB !important;
    border-radius: 12px !important;
    font-size: 14px !important;
    color: #111827 !important;
}

/* ── Streamlit button overrides ── */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    transition: all 0.15s !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar {
    width: 6px;
    height: 6px;
}
::-webkit-scrollbar-track {
    background: transparent;
}
::-webkit-scrollbar-thumb {
    background: #D1D5DB;
    border-radius: 3px;
}
</style>
""", unsafe_allow_html=True)


# ── Session state initialisation ───────────────────────────────────────────────
def init_state():
    defaults = {
        "page": "tasks",               # "tasks" | "session" | "connectors"
        "tasks": [],                   # list of task dicts
        "active_task_idx": None,       # int index into tasks
        "leads": [],
        "csv_download": None,
        "leads_csv_download": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()


# ── API key helpers ─────────────────────────────────────────────────────────────
def get_key(name, env_var):
    sk = f"_apikey_{name}"
    if sk not in st.session_state:
        st.session_state[sk] = os.getenv(env_var, "")
    return st.session_state[sk]

def set_key(name, value):
    st.session_state[f"_apikey_{name}"] = value


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

def web_search(query):
    try:
        url = f"https://api.duckduckgo.com/?q={quote_plus(query)}&format=json&no_html=1&skip_disambig=1"
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        results = []
        if data.get("AbstractText"):
            results.append(f"**Summary:** {data['AbstractText']}")
            if data.get("AbstractURL"):
                results.append(f"Source: {data['AbstractURL']}")
        for topic in data.get("RelatedTopics", [])[:5]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append(f"• {topic['Text']}")
                if topic.get("FirstURL"):
                    results.append(f"  {topic['FirstURL']}")
        if data.get("Answer"):
            results.append(f"**Answer:** {data['Answer']}")
        if not results:
            search_url = f"https://www.google.com/search?q={quote_plus(query)}"
            results.append(f"No direct results. Try searching: {search_url}")
        return {"results": "\n".join(results), "query": query}
    except Exception as e:
        return {"error": str(e)}


def apollo_search_people(titles=None, locations=None, keywords=None, num_results=20):
    apollo_key = get_key("apollo", "APOLLO_API_KEY")
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
        st.session_state.leads = leads
        # Also store in the active task
        if st.session_state.active_task_idx is not None:
            idx = st.session_state.active_task_idx
            st.session_state.tasks[idx]["leads"] = leads
        return {"leads": leads, "total": len(leads)}
    except Exception as e:
        return {"error": str(e)}


def apollo_enrich_person(first_name, last_name, company=None, domain=None):
    apollo_key = get_key("apollo", "APOLLO_API_KEY")
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
        return {"email": person.get("email", ""), "phone": person.get("phone", "")}
    except Exception as e:
        return {"error": str(e)}


def hubspot_create_contact(email, first_name="", last_name="", company="",
                            phone="", website="", job_title="", linkedin=""):
    hubspot_token = get_key("hubspot", "HUBSPOT_TOKEN")
    if not hubspot_token:
        return {"error": "HubSpot token not configured."}
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


def save_leads_csv():
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
    path = os.path.join(os.path.dirname(__file__), "outreach_drafts.csv")
    with open(path, "w", newline="") as f:
        f.write(csv_content)
    st.session_state["csv_download"] = csv_content
    return {"success": True, "path": path, "count": len(drafts)}


TOOL_MAP = {
    "web_search":             web_search,
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

def run_agent(user_message: str, task_messages: list):
    """
    Run Claude with tool use. Yields events:
      ("text", str)
      ("tool_start", tool_name)
      ("tool_end", (tool_name, result))
      ("done", None)
    """
    anthropic_key = get_key("anthropic", "ANTHROPIC_API_KEY")
    if not anthropic_key:
        yield "text", "Please configure your Anthropic API key in Settings (sidebar)."
        yield "done", None
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
    for msg in task_messages:
        if msg["role"] in ("user", "assistant") and isinstance(msg.get("content"), (str, list)):
            api_messages.append({"role": msg["role"], "content": msg["content"]})
    api_messages.append({"role": "user", "content": user_message})

    while True:
        response = client.messages.create(
            model="claude-opus-4-5",
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
            yield "text", full_text

        if response.stop_reason == "end_turn" or not tool_calls:
            break

        api_messages.append({"role": "assistant", "content": response.content})

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

    yield "done", None


# ── Sidebar ────────────────────────────────────────────────────────────────────

def render_sidebar():
    with st.sidebar:
        # Logo
        st.markdown("""
        <div class="sidebar-logo">
            <div class="sidebar-logo-icon">⚡</div>
            <div class="sidebar-logo-text">MMG Agent</div>
        </div>
        """, unsafe_allow_html=True)

        # New task button
        if st.button("+ New task", key="sb_new_task", use_container_width=True,
                     type="primary"):
            create_new_task()

        st.markdown('<div class="sidebar-nav-label">Navigation</div>', unsafe_allow_html=True)

        # Nav items
        pages = [
            ("tasks",      "✦", "Tasks"),
            ("connectors", "⬡", "Connectors"),
        ]
        for page_key, icon, label in pages:
            active_cls = "active" if st.session_state.page == page_key else ""
            # Use button for navigation
            btn_style = (
                "background:#1A2535;color:#FFFFFF;border:none;width:100%;text-align:left;"
                "padding:9px 16px;border-radius:8px;font-size:14px;font-weight:500;cursor:pointer;margin:2px 0;"
                if active_cls else
                "background:transparent;color:#8899AA;border:none;width:100%;text-align:left;"
                "padding:9px 16px;border-radius:8px;font-size:14px;font-weight:500;cursor:pointer;margin:2px 0;"
            )
            if st.button(f"{icon}  {label}", key=f"nav_{page_key}",
                         use_container_width=True):
                st.session_state.page = page_key
                st.session_state.active_task_idx = None
                st.rerun()

        st.markdown("<br>" * 2, unsafe_allow_html=True)

        # Settings (collapsible)
        with st.expander("⚙  Settings / API Keys"):
            ak = st.text_input("Anthropic API Key", type="password",
                               value=get_key("anthropic", "ANTHROPIC_API_KEY"),
                               key="inp_anthropic")
            if ak != get_key("anthropic", "ANTHROPIC_API_KEY"):
                set_key("anthropic", ak)

            apk = st.text_input("Apollo API Key", type="password",
                                value=get_key("apollo", "APOLLO_API_KEY"),
                                key="inp_apollo")
            if apk != get_key("apollo", "APOLLO_API_KEY"):
                set_key("apollo", apk)

            hst = st.text_input("HubSpot Token", type="password",
                                value=get_key("hubspot", "HUBSPOT_TOKEN"),
                                key="inp_hubspot")
            if hst != get_key("hubspot", "HUBSPOT_TOKEN"):
                set_key("hubspot", hst)

            if st.button("Test HubSpot", key="test_hs"):
                token = get_key("hubspot", "HUBSPOT_TOKEN")
                if not token:
                    st.error("No token entered.")
                else:
                    r = requests.get(
                        "https://api.hubapi.com/crm/v3/objects/contacts?limit=1",
                        headers={"Authorization": f"Bearer {token.strip()}"},
                    )
                    if r.status_code == 200:
                        st.success("HubSpot connected!")
                    else:
                        st.error(f"{r.status_code}: {r.json().get('message', r.text)}")

        # User profile pinned at bottom
        st.markdown("""
        <div class="sidebar-user">
            <div class="user-avatar">JG</div>
            <div>
                <div class="user-info-name">Jorge Garcia</div>
                <div class="user-info-role">Sales Manager</div>
            </div>
        </div>
        """, unsafe_allow_html=True)


# ── Task helpers ───────────────────────────────────────────────────────────────

def create_new_task(description="New task"):
    new_task = {
        "description": description,
        "status":      "In Progress",
        "outputs":     [],
        "time":        datetime.now(),
        "messages":    [],
        "leads":       [],
        "hubspot_synced": [],
    }
    st.session_state.tasks.insert(0, new_task)
    st.session_state.active_task_idx = 0
    st.session_state.page = "session"
    st.rerun()


def relative_time(dt: datetime) -> str:
    delta = datetime.now() - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "Just now"
    elif seconds < 3600:
        return f"{seconds // 60}m ago"
    elif seconds < 86400:
        return f"{seconds // 3600}h ago"
    else:
        return f"{seconds // 86400}d ago"


def status_badge(status: str) -> str:
    cls = {
        "Completed":   "badge-green",
        "In Progress": "badge-blue",
        "Failed":      "badge-red",
    }.get(status, "badge-gray")
    dot = {"Completed": "●", "In Progress": "●", "Failed": "●"}.get(status, "●")
    return f'<span class="badge {cls}">{dot} {status}</span>'


def output_pills(outputs: list) -> str:
    if not outputs:
        return '<span style="color:#9CA3AF;font-size:13px;">—</span>'
    html = ""
    for o in outputs:
        cls = "output-pill-blue" if "prospect" in o.lower() or "contact" in o.lower() else \
              "output-pill-green" if "email" in o.lower() else "output-pill"
        html += f'<span class="output-pill {cls}">{o}</span>'
    return html


# ── Page: Tasks ────────────────────────────────────────────────────────────────

def page_tasks():
    # Header
    st.markdown("""
    <div class="page-header">
        <div class="page-header-row">
            <div>
                <div class="page-title">Tasks</div>
                <div class="page-subtitle">History of all agent sessions and generated outputs.</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Top action bar
    col_space, col_btn = st.columns([8, 1.5])
    with col_btn:
        if st.button("+ New task", key="tasks_new_btn", type="primary", use_container_width=True):
            create_new_task()

    st.markdown("<br>", unsafe_allow_html=True)

    if not st.session_state.tasks:
        st.markdown("""
        <div class="empty-state">
            <div class="empty-state-icon">✦</div>
            <div class="empty-state-title">No tasks yet</div>
            <div class="empty-state-desc">Click "+ New task" to start your first agent session.</div>
        </div>
        """, unsafe_allow_html=True)
        return

    # Build table HTML
    rows_html = ""
    for i, task in enumerate(st.session_state.tasks):
        badge = status_badge(task["status"])
        pills = output_pills(task["outputs"])
        t = relative_time(task["time"])
        # Truncate description
        desc = task["description"]
        if len(desc) > 80:
            desc = desc[:77] + "..."
        rows_html += f"""
        <tr data-idx="{i}">
            <td><span style="font-weight:500;">{desc}</span></td>
            <td>{badge}</td>
            <td>{pills}</td>
            <td style="color:#6B7280;font-size:13px;">{t}</td>
        </tr>
        """

    table_html = f"""
    <div style="background:white;border:1px solid #E8ECF0;border-radius:12px;overflow:hidden;margin:0 0 32px 0;">
        <table class="tasks-table">
            <thead>
                <tr>
                    <th>TASK</th>
                    <th>STATUS</th>
                    <th>OUTPUTS</th>
                    <th>TIME</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)

    # Clickable rows via buttons
    st.markdown("**Open a session:**")
    for i, task in enumerate(st.session_state.tasks):
        desc = task["description"]
        if len(desc) > 60:
            desc = desc[:57] + "..."
        if st.button(f"Open: {desc}", key=f"open_task_{i}"):
            st.session_state.active_task_idx = i
            st.session_state.page = "session"
            st.rerun()


# ── Page: Agent Session ────────────────────────────────────────────────────────

def render_message(msg):
    """Render a single chat message with rich formatting."""
    role = msg["role"]
    content = msg["content"]

    if role == "user":
        if isinstance(content, list):
            # tool_result messages — skip in user-facing display
            return
        st.markdown(f"""
        <div class="chat-bubble-user">
            <div class="chat-bubble-user-inner">{content}</div>
        </div>
        """, unsafe_allow_html=True)

    elif role == "assistant":
        # Build inner content
        if isinstance(content, list):
            text_parts = [b.text for b in content if hasattr(b, "text") and b.text]
            display_content = " ".join(text_parts)
        else:
            display_content = content or ""

        # Escape HTML in display_content and convert newlines
        import html as html_lib
        safe_content = html_lib.escape(display_content).replace("\n", "<br>")

        # Check for Apollo leads in session
        task = None
        if st.session_state.active_task_idx is not None:
            task = st.session_state.tasks[st.session_state.active_task_idx]

        # Build extra widgets
        extras = ""

        # Apollo table if this message triggered a search (heuristic: message contains "Found")
        if task and task.get("leads") and ("found" in display_content.lower() or "lead" in display_content.lower()):
            leads = task["leads"][:10]  # show up to 10
            rows = ""
            for j, lead in enumerate(leads):
                loc = ", ".join(filter(None, [lead.get("city", ""), lead.get("state", "")]))
                score = 85 - j * 2  # synthetic relevance score
                rows += f"""
                <tr>
                    <td><strong>{lead.get('company','')}</strong><br>
                        <span style="color:#6B7280;font-size:12px;">{lead.get('industry','')}</span></td>
                    <td>{loc}</td>
                    <td style="font-family:monospace;font-size:12px;">{lead.get('phone','')}</td>
                    <td><span class="score-pill">{score}</span></td>
                </tr>
                """
            extras += f"""
            <table class="apollo-table" style="margin-top:12px;">
                <thead>
                    <tr><th>BUSINESS</th><th>LOCATION</th><th>PHONE</th><th>SCORE</th></tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            """

        # HubSpot sync list
        if task and task.get("hubspot_synced"):
            items = ""
            for contact in task["hubspot_synced"]:
                items += f"""
                <div class="sync-item">
                    <span class="sync-check">✓</span>
                    <span>{contact.get('company', contact.get('email',''))}</span>
                    <span class="sync-arrow">→</span>
                    <span style="color:#6B7280;">HubSpot Contact</span>
                </div>
                """
            extras += f'<div class="sync-list" style="margin-top:12px;">{items}</div>'

        st.markdown(f"""
        <div class="chat-bubble-agent">
            <div class="agent-avatar">⚡</div>
            <div class="agent-message-wrapper">
                <div class="agent-label">MMG Agent</div>
                <div class="agent-message-inner">
                    {safe_content}
                    {extras}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    elif role == "tool_event":
        event_type = msg.get("event_type")
        tool_name  = msg.get("tool_name", "")
        tool_icons = {
            "web_search":             "🌐",
            "apollo_search_people":   "🔍",
            "apollo_enrich_person":   "🔎",
            "hubspot_create_contact": "📥",
            "save_outreach_csv":      "💾",
        }
        icon = tool_icons.get(tool_name, "⚙️")
        if event_type == "start":
            st.markdown(f"""
            <div style="padding:4px 0;">
                <span class="tool-running">{icon} Running {tool_name}…</span>
            </div>
            """, unsafe_allow_html=True)
        else:
            result = msg.get("result", {})
            summary = ""
            if tool_name == "apollo_search_people" and "leads" in result:
                summary = f"Found {result['total']} leads"
            elif tool_name == "hubspot_create_contact":
                if result.get("success"):
                    summary = f"Created contact: {result.get('email','')}"
                else:
                    summary = f"Skipped: {result.get('error','')}"
            elif tool_name == "save_outreach_csv" and result.get("success"):
                summary = f"Saved {result['count']} drafts"
            elif tool_name == "web_search":
                summary = f"Search: {result.get('query','')}"
            else:
                summary = "Done"
            st.markdown(f"""
            <div style="padding:4px 0;">
                <span class="tool-done">✓ {tool_name}: {summary}</span>
            </div>
            """, unsafe_allow_html=True)


def page_session():
    task_idx = st.session_state.active_task_idx
    if task_idx is None or task_idx >= len(st.session_state.tasks):
        st.session_state.page = "tasks"
        st.rerun()
        return

    task = st.session_state.tasks[task_idx]

    # Header
    col_back, col_title = st.columns([1, 11])
    with col_back:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("← Back", key="session_back"):
            st.session_state.page = "tasks"
            st.session_state.active_task_idx = None
            st.rerun()

    st.markdown(f"""
    <div class="page-header" style="margin-top:-12px;">
        <div class="page-header-row">
            <div>
                <div class="page-title">Agent Session</div>
                <div class="page-subtitle">{task['description'][:80]}</div>
            </div>
            <div>
                {status_badge(task['status'])}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Messages
    chat_area = st.container()
    with chat_area:
        if not task["messages"]:
            st.markdown("""
            <div style="text-align:center;padding:60px 40px;color:#9CA3AF;">
                <div style="font-size:36px;margin-bottom:12px;">⚡</div>
                <div style="font-size:16px;font-weight:600;color:#374151;">MMG Agent ready</div>
                <div style="font-size:14px;margin-top:8px;">Type a message below to start your session.</div>
                <div style="font-size:13px;margin-top:20px;color:#6B7280;">
                    Try: "Find 10 nail salon owners in Miami" · "Load leads to HubSpot" · "Draft outreach emails"
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            for msg in task["messages"]:
                render_message(msg)

    # Download buttons
    col1, col2, col_space = st.columns([2, 2, 6])
    with col1:
        if st.session_state.get("leads_csv_download"):
            st.download_button("Download leads.csv", data=st.session_state["leads_csv_download"],
                               file_name="leads.csv", mime="text/csv", key="dl_leads")
    with col2:
        if st.session_state.get("csv_download"):
            st.download_button("Download outreach_drafts.csv", data=st.session_state["csv_download"],
                               file_name="outreach_drafts.csv", mime="text/csv", key="dl_outreach")

    st.markdown("<br>", unsafe_allow_html=True)

    # Chat input
    if prompt := st.chat_input("Ask a follow-up or give new instructions...", key="chat_input"):
        # Update task description on first message
        if not task["messages"]:
            task["description"] = prompt
            task["time"] = datetime.now()

        # Append user message
        task["messages"].append({"role": "user", "content": prompt})
        task["status"] = "In Progress"

        # Run agent and collect all events
        full_response = ""
        tool_events = []
        hubspot_synced_this_run = []

        with st.spinner("MMG Agent is thinking..."):
            for event_type, payload in run_agent(prompt, task["messages"][:-1]):
                if event_type == "text":
                    full_response += payload
                elif event_type == "tool_start":
                    tool_events.append({
                        "role": "tool_event",
                        "event_type": "start",
                        "tool_name": payload,
                    })
                elif event_type == "tool_end":
                    tool_name, result = payload
                    tool_events.append({
                        "role": "tool_event",
                        "event_type": "end",
                        "tool_name": tool_name,
                        "result": result,
                    })
                    # Track HubSpot syncs
                    if tool_name == "hubspot_create_contact" and result.get("success"):
                        hubspot_synced_this_run.append(result)
                    # Update outputs list
                    if tool_name == "apollo_search_people" and "leads" in result:
                        count = result["total"]
                        pill = f"{count} prospects"
                        if pill not in task["outputs"]:
                            task["outputs"].append(pill)
                    elif tool_name == "hubspot_create_contact" and result.get("success"):
                        # Count contacts
                        existing = next((o for o in task["outputs"] if "HubSpot" in o), None)
                        if existing:
                            task["outputs"].remove(existing)
                        current_count = len([
                            e for e in tool_events
                            if e.get("tool_name") == "hubspot_create_contact"
                            and e.get("result", {}).get("success")
                        ])
                        task["outputs"].append(f"{current_count} HubSpot contacts")
                    elif tool_name == "save_outreach_csv" and result.get("success"):
                        pill = f"{result['count']} email drafts"
                        existing = next((o for o in task["outputs"] if "email" in o), None)
                        if existing:
                            task["outputs"].remove(existing)
                        task["outputs"].append(pill)
                elif event_type == "done":
                    break

        # Append tool events and assistant response
        for ev in tool_events:
            task["messages"].append(ev)

        if hubspot_synced_this_run:
            task["hubspot_synced"] = task.get("hubspot_synced", []) + hubspot_synced_this_run

        if full_response:
            task["messages"].append({"role": "assistant", "content": full_response})

        task["status"] = "Completed"

        st.rerun()


# ── Page: Connectors ───────────────────────────────────────────────────────────

def page_connectors():
    st.title("Connectors")
    st.caption("Connect your tools. The agent uses these to take action on your behalf.")
    st.divider()

    has_hubspot = bool(get_key("hubspot", "HUBSPOT_TOKEN"))
    has_apollo  = bool(get_key("apollo", "APOLLO_API_KEY"))

    connectors = [
        {
            "icon": "🟠",
            "name": "HubSpot",
            "desc": "CRM platform. Sync contacts and companies automatically.",
            "meta": "Contacts synced via API",
            "connected": has_hubspot,
        },
        {
            "icon": "🔍",
            "name": "Apollo.io",
            "desc": "B2B lead database. Search companies and contacts.",
            "meta": "Organization search enabled",
            "connected": has_apollo,
        },
    ]

    connected  = [c for c in connectors if c["connected"]]
    available  = [c for c in connectors if not c["connected"]]

    def render_card(c):
        with st.container(border=True):
            col1, col2 = st.columns([1, 6])
            with col1:
                st.write(c["icon"])
            with col2:
                if c["connected"]:
                    st.markdown(f"**{c['name']}** &nbsp; ✅ Connected", unsafe_allow_html=True)
                else:
                    st.markdown(f"**{c['name']}**")
            st.caption(c["desc"])
            if c["meta"] and c["connected"]:
                st.caption(f"_{c['meta']}_")
            if c["connected"]:
                st.success("Active — configure key in Settings ↗", icon="🔗")
            else:
                st.info("Add API key in the Settings panel in the sidebar to connect.", icon="🔑")

    if connected:
        st.subheader(f"Connected ({len(connected)})")
        cols = st.columns(min(len(connected), 2))
        for i, c in enumerate(connected):
            with cols[i % 2]:
                render_card(c)
        st.divider()

    if available:
        st.subheader("Available")
        cols = st.columns(min(len(available), 2))
        for i, c in enumerate(available):
            with cols[i % 2]:
                render_card(c)


# ── Main layout ────────────────────────────────────────────────────────────────

render_sidebar()

# Routing
page = st.session_state.page

if page == "tasks":
    page_tasks()
elif page == "session":
    page_session()
elif page == "connectors":
    page_connectors()
else:
    page_tasks()
