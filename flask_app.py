import os
import re
import json
import csv
import io
from datetime import datetime
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from flask import Flask, request, session, Response, send_file, jsonify, render_template
import anthropic

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Module-level storage for leads and outreach (per process)
_leads_store    = []
_outreach_store = []
_perf_store     = []   # performance records [{provider, model, duration_ms, ...}]

# Approximate cost per 1M tokens (input, output) in USD
_MODEL_PRICING = {
    # Anthropic
    "claude-opus-4-6":            (5.00,  25.00),
    "claude-sonnet-4-6":          (3.00,  15.00),
    "claude-haiku-4-5":           (1.00,   5.00),
    "claude-opus-4-5":            (5.00,  25.00),
    "claude-sonnet-4-5":          (3.00,  15.00),
    "claude-opus-4-1":            (15.00, 75.00),
    # Gemini
    "gemini-2.5-flash-preview-04-17": (0.075, 0.30),
    "gemini-2.5-pro-preview-05-06":   (1.25,  5.00),
    "gemini-2.0-flash":               (0.075, 0.30),
    "gemini-1.5-flash":               (0.075, 0.30),
    "gemini-1.5-pro":                 (1.25,  5.00),
    "gemini-3-flash-preview":         (0.075, 0.30),
    "gemini-3.1-pro-preview":         (1.25,  5.00),
    "gemini-3.1-flash-lite-preview":  (0.25,  1.50),
    # Perplexity
    "sonar-pro":           (3.00, 15.00),
    "sonar":               (1.00,  1.00),
    "sonar-reasoning-pro": (2.00,  8.00),
    "sonar-reasoning":     (1.00,  5.00),
}

def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Return estimated USD cost for a single request."""
    price_in, price_out = _MODEL_PRICING.get(model, (1.00, 5.00))
    return round((input_tokens * price_in + output_tokens * price_out) / 1_000_000, 6)

def _record_perf(provider: str, model: str, duration_ms: int,
                 input_tokens: int, output_tokens: int,
                 tool_calls: int, leads_found: int, success: bool):
    global _perf_store
    _perf_store.append({
        "ts":            __import__("time").time(),
        "provider":      provider,
        "model":         model,
        "duration_ms":   duration_ms,
        "input_tokens":  input_tokens,
        "output_tokens": output_tokens,
        "tool_calls":    tool_calls,
        "leads_found":   leads_found,
        "success":       success,
        "cost_usd":      _estimate_cost(model, input_tokens, output_tokens),
    })
    # Keep last 200 records
    if len(_perf_store) > 200:
        _perf_store = _perf_store[-200:]

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Lead CSV fields ───────────────────────────────────────────────────────────

LEAD_FIELDS = [
    # Business identity
    "trade_name",
    "entity_name",
    "formation_date",
    "years_in_business",
    "sunbiz_status",
    "sunbiz_url",
    # Business contact
    "general_email",
    "business_phone",
    "address",
    "website",
    # Owner
    "owner_name",
    "owner_email",
    "owner_phone",
    # Registered Agent
    "registered_agent",
    "reg_agent_address",
    "reg_agent_email",
    "reg_agent_phone",
    # Social / reviews
    "instagram_url",
    "facebook_url",
    "google_review_count",
    "google_rating",
    # Extra
    "industry",
    "employees",
    "linkedin_url",
]


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "search_businesses_maps",
        "description": (
            "PRIMARY lead discovery tool. Searches Google Maps for businesses by keyword "
            "and location using a real browser. Returns structured data for each business: "
            "trade name, address, city, state, business phone, website URL, Google rating, "
            "and Google review count. Use this as Step 1 for every lead generation request."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword":     {"type": "string", "description": "Business type, e.g. 'nail salon'"},
                "location":    {"type": "string", "description": "City and state, e.g. 'Miami, FL'"},
                "num_results": {"type": "integer", "description": "Number of businesses to return (default 10, max 20)", "default": 10},
            },
            "required": ["keyword", "location"],
        },
    },
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
            "Only use this if the user explicitly asks for Apollo results."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keywords":    {"type": "string", "description": "Industry keywords, e.g. 'nail salon'"},
                "locations":   {"type": "array", "items": {"type": "string"},
                                "description": "Locations, e.g. ['Miami, FL']"},
                "num_results": {"type": "integer", "description": "Number of results (max 50)", "default": 20},
            },
            "required": ["keywords"],
        },
    },
    {
        "name": "enrich_leads_batch",
        "description": (
            "Enrich a list of leads with ALL required fields. ALWAYS call this after "
            "search_businesses_maps. It fills in every lead with: "
            "(1) Sunbiz: entity/corporate name, formation date, years in business, sunbiz status, "
            "owner name, registered agent name + address; "
            "(2) Website scrape: general email (info@...), Instagram URL, Facebook URL; "
            "(3) Google Maps: rating + review count; "
            "(4) Web search: owner email + cell phone, registered agent email + cell phone. "
            "NEVER call sunbiz_lookup / scrape_website_contact / get_google_reviews individually."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "leads": {
                    "type": "array",
                    "description": "List of lead objects from apollo_search_people",
                    "items": {
                        "type": "object",
                        "properties": {
                            "trade_name": {"type": "string"},
                            "website":    {"type": "string"},
                            "city":       {"type": "string"},
                            "state":      {"type": "string"},
                        },
                    },
                },
            },
            "required": ["leads"],
        },
    },
    {
        "name": "hubspot_create_contact",
        "description": "Create or update a contact in HubSpot CRM with full enriched lead data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "email":        {"type": "string"},
                "first_name":   {"type": "string"},
                "last_name":    {"type": "string"},
                "company":      {"type": "string"},
                "phone":        {"type": "string"},
                "website":      {"type": "string"},
                "job_title":    {"type": "string"},
                "linkedin":     {"type": "string"},
            },
            "required": ["email"],
        },
    },
    {
        "name": "save_leads_csv",
        "description": (
            "Save the fully enriched lead list to leads.csv. "
            "Call this after enriching leads with Sunbiz, website scraping, and Google reviews."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "leads": {
                    "type": "array",
                    "description": "List of enriched lead objects",
                    "items": {
                        "type": "object",
                        "properties": {
                            "trade_name":          {"type": "string"},
                            "entity_name":         {"type": "string"},
                            "formation_date":      {"type": "string"},
                            "years_in_business":   {"type": "string"},
                            "general_email":       {"type": "string"},
                            "owner_name":          {"type": "string"},
                            "owner_email":         {"type": "string"},
                            "owner_phone":         {"type": "string"},
                            "registered_agent":    {"type": "string"},
                            "reg_agent_address":   {"type": "string"},
                            "reg_agent_email":     {"type": "string"},
                            "reg_agent_phone":     {"type": "string"},
                            "business_phone":      {"type": "string"},
                            "address":             {"type": "string"},
                            "website":             {"type": "string"},
                            "instagram_url":       {"type": "string"},
                            "facebook_url":        {"type": "string"},
                            "google_review_count": {"type": "string"},
                            "google_rating":       {"type": "string"},
                            "industry":            {"type": "string"},
                            "employees":           {"type": "string"},
                            "linkedin_url":        {"type": "string"},
                            "sunbiz_url":          {"type": "string"},
                            "sunbiz_status":       {"type": "string"},
                        },
                    },
                }
            },
            "required": ["leads"],
        },
    },
    {
        "name": "get_collected_leads",
        "description": (
            "Return the leads that were already collected and enriched in this session. "
            "Use this when the user asks to upload leads to HubSpot, export, or do anything "
            "with previously found leads. Do NOT search for new leads in that case."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "save_outreach_csv",
        "description": "Save email drafts to outreach_drafts.csv.",
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

def search_businesses_maps(keyword, location, num_results=10):
    """
    Search Google Maps for businesses using a headless browser.
    Returns structured lead data: name, address, city, state, phone,
    website, Google rating, Google review count.
    """
    from playwright.sync_api import sync_playwright
    import time

    num_results = min(int(num_results or 10), 20)
    query = f"{keyword} {location}"
    businesses = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()

            search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)

            # ── Collect card URLs upfront (before any navigation makes them stale) ──
            # Fetch extra cards as buffer in case some fail
            fetch_count = num_results + 3
            cards = page.query_selector_all("a.hfpxzc")
            card_urls = []
            for c in cards[:fetch_count]:
                href = c.get_attribute("href") or ""
                if href and href.startswith("https://"):
                    card_urls.append(href)

            for url in card_urls:
                if len(businesses) >= num_results:
                    break
                try:
                    # Navigate directly by URL — no stale element issues
                    page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    time.sleep(3)

                    html = page.content()

                    # ── Business name ──
                    name_el = page.query_selector("h1.DUwDvf, h1.fontHeadlineLarge")
                    name = name_el.inner_text().strip() if name_el else ""

                    # ── Address ──
                    addr_el = page.query_selector(
                        '[data-item-id="address"] .Io6YTe, '
                        'button[data-item-id="address"] .Io6YTe'
                    )
                    address = addr_el.inner_text().strip() if addr_el else ""

                    # ── Parse city/state from address ──
                    city, state = "", ""
                    if address:
                        parts = address.split(",")
                        if len(parts) >= 2:
                            city = parts[-2].strip()
                        state_zip = parts[-1].strip() if parts else ""
                        state_m = re.search(r'\b([A-Z]{2})\b', state_zip)
                        state = state_m.group(1) if state_m else ""

                    # ── Phone ──
                    phone_el = page.query_selector(
                        '[data-item-id*="phone:tel"] .Io6YTe, '
                        'button[data-tooltip="Copy phone number"] .Io6YTe'
                    )
                    phone = phone_el.inner_text().strip() if phone_el else ""

                    # ── Website ──
                    website_el = page.query_selector(
                        'a[data-item-id="authority"], '
                        'a[aria-label*="website" i]'
                    )
                    website = ""
                    if website_el:
                        href = website_el.get_attribute("href") or ""
                        if href and not href.startswith("https://www.google"):
                            website = href.split("?")[0]

                    # ── Rating + Review count ── multi-strategy extraction
                    rating = ""
                    count  = ""

                    # Strategy 1: aria-label on any element containing "stars"
                    for el in page.query_selector_all('[aria-label]'):
                        try:
                            lbl = el.get_attribute("aria-label") or ""
                            if not rating:
                                rm = re.search(r'(\d[\.,]\d)\s*stars?', lbl, re.IGNORECASE)
                                if rm:
                                    rating = rm.group(1).replace(",", ".")
                                else:
                                    rm2 = re.search(r'^(\d)\s*stars?$', lbl.strip(), re.IGNORECASE)
                                    if rm2:
                                        rating = rm2.group(1)
                            if not count:
                                cm = re.search(r'([\d,]+)\s*reviews?', lbl, re.IGNORECASE)
                                if cm:
                                    count = cm.group(1).replace(",", "")
                            if rating and count:
                                break
                        except Exception:
                            continue

                    # Strategy 2: regex scan on full page HTML
                    if not rating:
                        rm = re.search(r'(\d[\.,]\d)\s*stars?', html, re.IGNORECASE)
                        if rm:
                            rating = rm.group(1).replace(",", ".")
                    if not count:
                        cm = re.search(r'([\d,]+)\s*reviews?', html, re.IGNORECASE)
                        if cm:
                            count = cm.group(1).replace(",", "")

                    # Strategy 3: visible text in known rating container elements
                    if not rating:
                        for sel in ('span.ceNzKf', 'div.F7nice > span', 'span.fontBodyMedium'):
                            try:
                                el = page.query_selector(sel)
                                if el:
                                    txt = el.inner_text().strip()
                                    rm = re.search(r'(\d[\.,]\d)', txt)
                                    if rm:
                                        rating = rm.group(1).replace(",", ".")
                                        break
                            except Exception:
                                continue

                    if name:
                        businesses.append({
                            "trade_name":          name,
                            "entity_name":         "",
                            "formation_date":      "",
                            "years_in_business":   "",
                            "sunbiz_status":       "",
                            "sunbiz_url":          "",
                            "general_email":       "",
                            "business_phone":      phone,
                            "address":             address,
                            "city":                city,
                            "state":               state,
                            "website":             website,
                            "owner_name":          "",
                            "owner_email":         "",
                            "owner_phone":         "",
                            "registered_agent":    "",
                            "reg_agent_address":   "",
                            "reg_agent_email":     "",
                            "reg_agent_phone":     "",
                            "instagram_url":       "",
                            "facebook_url":        "",
                            "google_rating":       rating,
                            "google_review_count": count,
                            "industry":            keyword,
                            "employees":           "",
                            "linkedin_url":        "",
                        })

                except Exception:
                    continue

            browser.close()

    except Exception as e:
        return {"error": str(e), "businesses": []}

    return {"leads": businesses, "total": len(businesses)}


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
        for topic in data.get("RelatedTopics", [])[:6]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append(f"- {topic['Text']}")
                if topic.get("FirstURL"):
                    results.append(f"  {topic['FirstURL']}")
        if data.get("Answer"):
            results.append(f"Answer: {data['Answer']}")
        if not results:
            results.append(f"No direct results. Try: https://www.google.com/search?q={quote_plus(query)}")
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
            # Phone — try multiple fields
            phone = org.get("phone") or ""
            if not phone:
                pp = org.get("primary_phone") or {}
                phone = pp.get("sanitized_number") or pp.get("number") or ""

            # Facebook URL — Apollo sometimes returns this
            fb_url = org.get("facebook_url") or ""

            # Founded year → formation date approximation
            founded_year = org.get("founded_year") or ""
            formation_date = f"01/01/{founded_year}" if founded_year else ""
            years_in_business = ""
            if founded_year:
                try:
                    years_in_business = str(datetime.now().year - int(founded_year))
                except Exception:
                    pass

            lead = {
                "trade_name":        org.get("name", ""),
                "entity_name":       "",   # filled by sunbiz_lookup
                "formation_date":    formation_date,
                "years_in_business": years_in_business,
                "general_email":     "",   # filled by scrape_website_contact
                "owner_name":        "",
                "owner_email":       "",
                "owner_phone":       "",
                "registered_agent":  "",   # filled by sunbiz_lookup
                "reg_agent_address": "",
                "business_phone":    phone,
                "address":           org.get("raw_address", ""),
                "website":           org.get("website_url", ""),
                "instagram_url":     "",   # filled by scrape_website_contact
                "facebook_url":      fb_url,
                "google_review_count": "",
                "google_rating":     "",
                "industry":          org.get("industry", ""),
                "employees":         str(org.get("estimated_num_employees", "")),
                "linkedin_url":      org.get("linkedin_url", ""),
                "sunbiz_url":        "",
                "sunbiz_status":     "",
            }
            leads.append(lead)

        _leads_store = leads
        _save_leads_to_file(leads)
        return {"leads": leads, "total": len(leads)}
    except Exception as e:
        return {"error": str(e)}


def sunbiz_lookup(business_name):
    """
    Search Florida Sunbiz corporate registry using a headless browser
    (required to bypass Cloudflare protection on search.sunbiz.org).
    Returns entity name, formation date, status, registered agent, and owner.
    """
    from playwright.sync_api import sync_playwright
    import time

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()

            # ── Step 1: submit the search form ──
            page.goto(
                "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName",
                wait_until="domcontentloaded", timeout=30000,
            )
            time.sleep(0.5)
            page.fill("#SearchTerm", business_name)
            page.click("input[type=submit]")
            page.wait_for_load_state("domcontentloaded", timeout=15000)
            time.sleep(1.5)

            html = page.content()

            # Collect all result links + their display names
            result_pairs = re.findall(
                r'href="(/Inquiry/CorporationSearch/SearchResultDetail[^"]+)"[^>]*>\s*([^<]+?)\s*</a>',
                html,
            )
            if not result_pairs:
                browser.close()
                return {"found": False, "searched": business_name}

            # Pick the best-matching result using word-level overlap scoring
            def _name_score(search, candidate):
                """Score candidate against search using word overlap (higher = better match)."""
                s_words = set(re.sub(r"[^a-z0-9\s]", "", search.lower()).split())
                c_words = set(re.sub(r"[^a-z0-9\s]", "", candidate.lower().replace("&amp;", "")).split())
                # Remove common noise words
                noise = {"llc", "inc", "corp", "ltd", "co", "the", "a", "of", "and", "&"}
                s_core = s_words - noise
                c_core = c_words - noise
                if not s_core:
                    return 0
                # Exact word matches weighted more heavily
                exact = len(s_core & c_core)
                # Partial/substring matches
                partial = sum(1 for sw in s_core for cw in c_core if sw in cw or cw in sw) - exact
                # Penalize length difference
                length_penalty = abs(len(s_core) - len(c_core)) * 0.1
                return exact * 2 + partial * 0.5 - length_penalty

            best_href, best_score = result_pairs[0][0], -1
            for href, name in result_pairs:
                score = _name_score(business_name, name)
                if score > best_score:
                    best_score = score
                    best_href = href

            # ── Step 2: load the detail page ──
            detail_url = "https://search.sunbiz.org" + best_href.replace("&amp;", "&")
            page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(1.5)
            html = page.content()
            browser.close()

        # ── Parse entity type + corporate name ──
        corp_m = re.search(
            r'<div[^>]*class="[^"]*corporationName[^"]*"[^>]*>.*?<p>([^<]+)</p>\s*<p>([^<]+)</p>',
            html, re.DOTALL,
        )
        entity_type = corp_m.group(1).strip() if corp_m else ""
        entity_name = (
            corp_m.group(2).strip().replace("&amp;", "&") if corp_m else ""
        )

        # ── Parse filing info (label → span pairs) ──
        filing = {}
        for label, value in re.findall(
            r'<label[^>]*>\s*([^<]+?)\s*</label>\s*<span>\s*([^<]*?)\s*</span>', html
        ):
            filing[label.strip()] = value.strip()

        date_filed    = filing.get("Date Filed", "")
        status        = filing.get("Status", "")
        doc_number    = filing.get("Document Number", "")

        # ── Years in business ──
        years_in_business = ""
        if date_filed:
            try:
                filed_dt = datetime.strptime(date_filed, "%m/%d/%Y")
                years_in_business = str(
                    round((datetime.now() - filed_dt).days / 365.25, 1)
                )
            except Exception:
                pass

        def _section_text(title, html_body):
            """Extract visible text from a named detailSection."""
            m = re.search(
                rf"<span>\s*{re.escape(title)}\s*</span>(.*?)(?=<div[^>]*class=\"detailSection|$)",
                html_body, re.DOTALL | re.IGNORECASE,
            )
            if not m:
                return []
            chunk = m.group(1)
            chunk = re.sub(r"<br\s*/?>", "\n", chunk)
            chunk = re.sub(r"<[^>]+>", "", chunk)
            chunk = chunk.replace("&amp;", "&").replace("&nbsp;", " ")
            return [l.strip() for l in chunk.splitlines() if l.strip()]

        # ── Registered Agent ──
        ra_lines = _section_text("Registered Agent Name &amp; Address", html)
        reg_agent      = ra_lines[0] if ra_lines else ""
        reg_agent_addr = ", ".join(ra_lines[1:]) if len(ra_lines) > 1 else ""

        # ── Principal Address ──
        pa_lines       = _section_text("Principal Address", html)
        principal_addr = ", ".join(pa_lines)

        # ── Officers ──
        owner_name = ""
        off_m = re.search(
            r"<span>\s*Officer/Director Detail\s*</span>(.*?)(?=<div[^>]*class=\"detailSection|$)",
            html, re.DOTALL | re.IGNORECASE,
        )
        if off_m:
            off_chunk = re.sub(r"<br\s*/?>", "\n", off_m.group(1))
            off_chunk = re.sub(r"<[^>]+>", "\n", off_chunk)
            off_chunk = off_chunk.replace("&amp;", "&").replace("&nbsp;", " ")
            off_lines = [l.strip() for l in off_chunk.splitlines() if l.strip()]
            # Officer names come after a "Title X" line
            for i, line in enumerate(off_lines):
                if line.lower().startswith("title") and i + 1 < len(off_lines):
                    candidate = off_lines[i + 1]
                    # Must look like a name (all caps, letters)
                    if re.match(r"[A-Z][A-Z ,.\-']+$", candidate):
                        owner_name = candidate
                        break

        return {
            "found":             True,
            "sunbiz_url":        detail_url,
            "entity_type":       entity_type,
            "entity_name":       entity_name,
            "document_number":   doc_number,
            "date_filed":        date_filed,
            "years_in_business": years_in_business,
            "sunbiz_status":     status,
            "principal_address": principal_addr,
            "registered_agent":  reg_agent,
            "reg_agent_address": reg_agent_addr,
            "owner_name":        owner_name,
        }

    except Exception as e:
        return {"error": str(e), "searched": business_name}


def scrape_website_contact(url):
    """Visit a business website and extract email, Instagram, Facebook, phone."""
    if not url:
        return {"error": "No URL provided"}

    # Normalise URL
    if not url.startswith("http"):
        url = "https://" + url

    emails      = set()
    instagram   = ""
    facebook    = ""
    phones      = set()

    # Pages to attempt
    base = url.rstrip("/")
    pages = [base, base + "/contact", base + "/about", base + "/contact-us"]

    for page_url in pages:
        try:
            resp = requests.get(
                page_url, headers=SCRAPE_HEADERS,
                timeout=10, allow_redirects=True,
            )
            if resp.status_code >= 400:
                continue
            html = resp.text

            # ── Emails ──
            found = re.findall(
                r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b', html
            )
            noise = {
                "example", "sentry", "wixpress", "squarespace", "wordpress",
                "schema", "domain", "email", "support@sentry", "noreply",
                "webmaster", "user@",
            }
            for e in found:
                el = e.lower()
                if not any(n in el for n in noise) and len(el) < 80:
                    emails.add(el)

            # ── Instagram ──
            if not instagram:
                ig = re.search(
                    r'(?:href|content)="https?://(?:www\.)?instagram\.com/([^/"?#\s]+)',
                    html, re.IGNORECASE,
                )
                if ig and ig.group(1) not in ("p", "explore", "accounts", "stories"):
                    instagram = f"https://www.instagram.com/{ig.group(1)}"

            # ── Facebook ──
            if not facebook:
                fb = re.search(
                    r'(?:href|content)="https?://(?:www\.)?facebook\.com/([^/"?#\s]+)',
                    html, re.IGNORECASE,
                )
                if fb:
                    handle = fb.group(1)
                    skip = {"sharer", "share", "dialog", "plugins", "login", "groups", "events"}
                    if handle not in skip:
                        facebook = f"https://www.facebook.com/{handle}"

            # ── Phones ──
            tel_links = re.findall(r'href="tel:([^"]+)"', html, re.IGNORECASE)
            for p in tel_links:
                clean = re.sub(r"[^\d+]", "", p)
                if len(clean) >= 10:
                    phones.add(p.strip())

        except Exception:
            continue

    # If requests-based scraping found nothing useful, try Playwright for JS-heavy sites
    if not emails and not instagram and not facebook and not phones:
        try:
            from playwright.sync_api import sync_playwright
            import time as _time
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                ctx = browser.new_context(user_agent=SCRAPE_HEADERS["User-Agent"])
                pg = ctx.new_page()
                for page_url in pages[:2]:  # just home + /contact
                    try:
                        pg.goto(page_url, wait_until="domcontentloaded", timeout=15000)
                        _time.sleep(1)
                        html = pg.content()
                        found = re.findall(
                            r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b', html
                        )
                        noise = {
                            "example", "sentry", "wixpress", "squarespace", "wordpress",
                            "schema", "domain", "email", "support@sentry", "noreply",
                            "webmaster", "user@",
                        }
                        for e in found:
                            el = e.lower()
                            if not any(n in el for n in noise) and len(el) < 80:
                                emails.add(el)
                        if not instagram:
                            ig = re.search(
                                r'(?:href|content)="https?://(?:www\.)?instagram\.com/([^/"?#\s]+)',
                                html, re.IGNORECASE,
                            )
                            if ig and ig.group(1) not in ("p", "explore", "accounts", "stories"):
                                instagram = f"https://www.instagram.com/{ig.group(1)}"
                        if not facebook:
                            fb = re.search(
                                r'(?:href|content)="https?://(?:www\.)?facebook\.com/([^/"?#\s]+)',
                                html, re.IGNORECASE,
                            )
                            if fb:
                                handle = fb.group(1)
                                skip = {"sharer", "share", "dialog", "plugins", "login", "groups", "events"}
                                if handle not in skip:
                                    facebook = f"https://www.facebook.com/{handle}"
                        tel_links = re.findall(r'href="tel:([^"]+)"', html, re.IGNORECASE)
                        for p in tel_links:
                            clean = re.sub(r"[^\d+]", "", p)
                            if len(clean) >= 10:
                                phones.add(p.strip())
                        if emails or instagram or facebook or phones:
                            break
                    except Exception:
                        continue
                browser.close()
        except Exception:
            pass

    # Prefer info@, contact@, hello@ style emails as "general email"
    priority_prefixes = ("info", "contact", "hello", "office", "admin", "mail", "booking")
    general_email = ""
    for e in emails:
        if any(e.startswith(p + "@") for p in priority_prefixes):
            general_email = e
            break
    if not general_email and emails:
        general_email = sorted(emails)[0]

    return {
        "general_email":  general_email,
        "all_emails":     sorted(emails)[:6],
        "instagram_url":  instagram,
        "facebook_url":   facebook,
        "phones":         list(phones)[:3],
    }


def get_google_reviews(business_name, city="", state=""):
    """
    Use a headless browser to open Google Maps, click the first result,
    and extract the business's star rating and Google review count
    from aria-label attributes in the detail panel.
    """
    from playwright.sync_api import sync_playwright
    import time

    query  = f"{business_name} {city} {state}".strip()
    rating = ""
    count  = ""

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            page.goto(
                f"https://www.google.com/maps/search/{quote_plus(query)}",
                wait_until="domcontentloaded", timeout=20000,
            )
            time.sleep(2)

            # Click the first result to open the business detail panel
            first = page.query_selector("a.hfpxzc")
            if first:
                first.click()
                time.sleep(4)

            html = page.content()
            browser.close()

        # Strategy 1: scan all aria-label attributes
        for el in page.query_selector_all('[aria-label]'):
            try:
                lbl = el.get_attribute("aria-label") or ""
                if not rating:
                    rm = re.search(r'(\d[\.,]\d)\s*stars?', lbl, re.IGNORECASE)
                    if rm:
                        rating = rm.group(1).replace(",", ".")
                    else:
                        rm2 = re.search(r'^(\d)\s*stars?$', lbl.strip(), re.IGNORECASE)
                        if rm2:
                            rating = rm2.group(1)
                if not count:
                    cm = re.search(r'([\d,]+)\s*reviews?', lbl, re.IGNORECASE)
                    if cm:
                        count = cm.group(1).replace(",", "")
                if rating and count:
                    break
            except Exception:
                continue

        # Strategy 2: regex scan on full HTML
        if not rating:
            rm = re.search(r'(\d[\.,]\d)\s*stars?', html, re.IGNORECASE)
            if rm:
                rating = rm.group(1).replace(",", ".")
        if not count:
            cm = re.search(r'([\d,]+)\s*reviews?', html, re.IGNORECASE)
            if cm:
                count = cm.group(1).replace(",", "")

        # Strategy 3: visible text in known containers
        if not rating:
            for sel in ('span.ceNzKf', 'div.F7nice > span', 'span.fontBodyMedium'):
                try:
                    el = page.query_selector(sel)
                    if el:
                        txt = el.inner_text().strip()
                        rm = re.search(r'(\d[\.,]\d)', txt)
                        if rm:
                            rating = rm.group(1).replace(",", ".")
                            break
                except Exception:
                    continue

    except Exception:
        pass

    return {
        "google_rating":       rating,
        "google_review_count": count,
    }


def hubspot_create_contact(email, first_name="", last_name="", company="",
                            phone="", website="", job_title="", linkedin="",
                            _hubspot_token=None):
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
            return {"success": False, "error": "Contact already exists", "email": email}
        else:
            return {"success": False, "error": data.get("message", str(data)), "email": email}
    except Exception as e:
        return {"error": str(e)}


def get_collected_leads():
    """Return leads already collected in this session."""
    global _leads_store
    if not _leads_store:
        return {"leads": [], "count": 0, "message": "No leads collected yet in this session."}
    return {"leads": _leads_store, "count": len(_leads_store)}


def save_leads_csv(leads):
    """Save enriched leads list to leads.csv."""
    global _leads_store
    _leads_store = leads
    _save_leads_to_file(leads)
    return {"success": True, "count": len(leads), "path": "leads.csv"}


def _save_leads_to_file(leads):
    try:
        path = os.path.join(os.path.dirname(__file__), "leads.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=LEAD_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(leads)
    except Exception:
        pass


def save_outreach_csv(drafts):
    global _outreach_store
    _outreach_store = drafts
    try:
        path = os.path.join(os.path.dirname(__file__), "outreach_drafts.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["name", "email", "subject_line", "email_body"],
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(drafts)
        return {"success": True, "path": path, "count": len(drafts)}
    except Exception as e:
        return {"error": str(e)}


def _find_person_contact(name, business_name="", city="", state=""):
    """
    Web-search for a person's email and phone number.
    Used for owner and registered agent contact lookup.
    Returns {"email": "...", "phone": "..."}.
    """
    if not name:
        return {"email": "", "phone": ""}

    noise_domains = {"example", "sentry", "wixpress", "squarespace", "domain", "noreply",
                     "wordpress", "schema", "w3.org", "google", "yelp", "facebook"}

    def _extract(text):
        emails = re.findall(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b', text)
        email = ""
        for e in emails:
            el = e.lower()
            if not any(n in el for n in noise_domains) and len(el) < 60:
                email = el
                break
        phones = re.findall(r'\(?\d{3}\)?[\s\.\-]?\d{3}[\s\.\-]?\d{4}', text)
        phone = phones[0].strip() if phones else ""
        return email, phone

    # Try multiple query strategies
    queries = []
    if business_name:
        queries.append(f'"{name}" "{business_name}" email contact')
        queries.append(f'"{name}" {city} {state} {business_name} owner email')
    queries.append(f'"{name}" {city} {state} email phone')

    for query in queries:
        try:
            result = web_search(query.strip())
            text = result.get("results", "")
            email, phone = _extract(text)
            if email or phone:
                return {"email": email, "phone": phone}
        except Exception:
            continue

    return {"email": "", "phone": ""}


def enrich_leads_batch(leads):
    """
    Enrich every lead in the list with Sunbiz, website contact info, and
    Google Maps reviews — all in one tool call.  Yields progress via a
    shared list; returns the fully enriched leads list and saves to CSV.
    """
    import concurrent.futures

    enriched = []

    def _enrich_one(lead):
        result = dict(lead)
        # Ensure all LEAD_FIELDS keys exist (blank by default)
        for f in LEAD_FIELDS:
            result.setdefault(f, "")

        name  = result.get("trade_name", "")
        url   = result.get("website", "")
        city  = result.get("city", "")
        state = result.get("state", "")

        # 1. Sunbiz — entity name, formation date, owner, registered agent
        try:
            sb = sunbiz_lookup(name)
            if sb.get("found"):
                result["entity_name"]       = sb.get("entity_name", "")
                result["formation_date"]    = sb.get("date_filed", "")
                result["years_in_business"] = sb.get("years_in_business", "")
                result["sunbiz_status"]     = sb.get("sunbiz_status", "")
                result["sunbiz_url"]        = sb.get("sunbiz_url", "")
                result["registered_agent"]  = sb.get("registered_agent", "")
                result["reg_agent_address"] = sb.get("reg_agent_address", "")
                if sb.get("owner_name") and not result.get("owner_name"):
                    result["owner_name"] = sb.get("owner_name", "")
        except Exception:
            pass

        # 2. Website scrape — general email, Instagram, Facebook
        if url:
            try:
                ws = scrape_website_contact(url)
                if ws.get("general_email"):
                    result["general_email"] = ws["general_email"]
                if ws.get("instagram_url"):
                    result["instagram_url"] = ws["instagram_url"]
                if ws.get("facebook_url") and not result.get("facebook_url"):
                    result["facebook_url"] = ws["facebook_url"]
                # Pull any phone from website if business_phone still blank
                if not result.get("business_phone") and ws.get("phones"):
                    result["business_phone"] = ws["phones"][0]
            except Exception:
                pass

        # 3. Google reviews — rating + count (skip if already populated from Maps)
        if not (result.get("google_rating") and result.get("google_review_count")):
            try:
                gr = get_google_reviews(name, city, state)
                if gr.get("google_rating"):
                    result["google_rating"] = gr["google_rating"]
                if gr.get("google_review_count"):
                    result["google_review_count"] = gr["google_review_count"]
            except Exception:
                pass

        # 4. Owner contact — email + cell phone via web search
        owner = result.get("owner_name", "")
        if owner and not (result.get("owner_email") and result.get("owner_phone")):
            try:
                oc = _find_person_contact(owner, name, city, state)
                if oc.get("email") and not result.get("owner_email"):
                    result["owner_email"] = oc["email"]
                if oc.get("phone") and not result.get("owner_phone"):
                    result["owner_phone"] = oc["phone"]
            except Exception:
                pass

        # 5. Registered agent contact — email + cell phone via web search
        agent = result.get("registered_agent", "")
        if agent and not (result.get("reg_agent_email") and result.get("reg_agent_phone")):
            try:
                ac = _find_person_contact(agent, name, city, state)
                if ac.get("email") and not result.get("reg_agent_email"):
                    result["reg_agent_email"] = ac["email"]
                if ac.get("phone") and not result.get("reg_agent_phone"):
                    result["reg_agent_phone"] = ac["phone"]
            except Exception:
                pass

        return result

    # Run enrichment in parallel (3 workers to avoid overloading browsers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(_enrich_one, lead): i for i, lead in enumerate(leads)}
        for future in concurrent.futures.as_completed(futures):
            try:
                enriched.append(future.result())
            except Exception:
                pass

    # Sort back to original order by trade_name
    enriched.sort(key=lambda x: x.get("trade_name", ""))

    # Save to CSV
    global _leads_store
    _leads_store = enriched
    _save_leads_to_file(enriched)

    return {
        "leads": enriched,
        "total": len(enriched),
        "saved": True,
    }


TOOL_MAP = {
    "search_businesses_maps": search_businesses_maps,
    "web_search":             web_search,
    "apollo_search_people":   apollo_search_people,
    "enrich_leads_batch":     enrich_leads_batch,
    "get_collected_leads":    get_collected_leads,
    "sunbiz_lookup":          sunbiz_lookup,
    "scrape_website_contact": scrape_website_contact,
    "get_google_reviews":     get_google_reviews,
    "hubspot_create_contact": hubspot_create_contact,
    "save_leads_csv":         save_leads_csv,
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

SYSTEM_PROMPT = """You are MMG Agent, a lead generation assistant for a commercial real estate broker.

## Required fields — pull these for EVERY lead, every time, no exceptions

1.  Business trade name
2.  Business entity / corporate name (from Sunbiz)
3.  Company formation date + years in business
4.  Business general email (e.g. info@salon.com — from website)
5.  Owner name (from Sunbiz officers section)
6.  Owner email
7.  Owner cell phone (for HubSpot texting)
8.  Registered Agent name (from Sunbiz)
9.  Registered Agent email
10. Registered Agent cell phone (for HubSpot texting)
11. Business address
12. Business phone
13. Website URL
14. Instagram URL + Facebook URL
15. Google rating + Google review count

## Workflow

**Finding new leads:**
Step 1 — Call search_businesses_maps with the keyword, location, and exact num_results requested.
Step 2 — Immediately pass the full `leads` array into enrich_leads_batch (fills all 15 fields in parallel).
Step 3 — Reply with ONE sentence: "Found and enriched N [type] in [location] — results are in the table below."
Never call sunbiz_lookup, scrape_website_contact, or get_google_reviews individually.
Only use apollo_search_people if the user explicitly asks for it.

**Uploading to HubSpot / working with existing leads:**
NEVER search for new leads. NEVER enrich leads. Instead:
Step 1 — Call get_collected_leads() to retrieve the already-enriched leads from this session.
         The UI will render the table automatically — do NOT describe or list the leads in your response.
Step 2 — For each lead, call hubspot_create_contact using: owner_name (split into first/last), owner_email, company=trade_name, phone=owner_phone or business_phone, website, job_title="Owner".
Step 3 — Reply with ONE sentence summarising how many contacts were uploaded.

## Rules
- Keep ALL post-tool responses to 1 sentence.
- Do not use web_search unless the user explicitly asks.
- No markdown tables, no field lists — the UI handles display.
"""


# ── Convert TOOLS → OpenAI / Gemini format ───────────────────────────────────

def _tools_openai_fmt():
    """Reformat TOOLS list from Anthropic schema to OpenAI function-calling schema."""
    return [
        {
            "type": "function",
            "function": {
                "name":        t["name"],
                "description": t["description"],
                "parameters":  t["input_schema"],
            },
        }
        for t in TOOLS
    ]


# ── Generic OpenAI-compatible agent loop ─────────────────────────────────────

def _run_agent_openai_compat(user_message: str, history: list,
                              api_key: str, model: str, base_url: str,
                              provider: str = "gemini",
                              apollo_key="", hubspot_token=""):
    """Shared agent loop for any OpenAI-compatible endpoint (Gemini, etc.)."""
    import time as _time
    from openai import OpenAI as _OAI

    client    = _OAI(api_key=api_key, base_url=base_url)
    oai_tools = _tools_openai_fmt()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        role    = msg.get("role")
        content = msg.get("content")
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_message})

    t0 = _time.time()
    success = False
    total_in = total_out = total_tools = total_leads = 0
    try:
        while True:
            response = client.chat.completions.create(
                model=model, messages=messages,
                tools=oai_tools, tool_choice="auto",
            )
            usage = getattr(response, "usage", None)
            if usage:
                total_in  += getattr(usage, "prompt_tokens",     0) or 0
                total_out += getattr(usage, "completion_tokens", 0) or 0

            choice  = response.choices[0]
            msg_obj = choice.message

            if msg_obj.content:
                yield f"data: {json.dumps({'type': 'text', 'content': msg_obj.content})}\n\n"

            if choice.finish_reason == "stop" or not msg_obj.tool_calls:
                break

            messages.append(msg_obj)
            total_tools += len(msg_obj.tool_calls)

            tool_results = []
            for tc in msg_obj.tool_calls:
                name = tc.function.name
                try:
                    inputs = json.loads(tc.function.arguments)
                except Exception:
                    inputs = {}

                yield f"data: {json.dumps({'type': 'tool_start', 'name': name})}\n\n"
                result = run_tool(name, inputs,
                                  apollo_key=apollo_key,
                                  hubspot_token=hubspot_token)
                yield f"data: {json.dumps({'type': 'tool_end', 'name': name, 'result': result})}\n\n"
                if isinstance(result, dict) and result.get("leads"):
                    total_leads += len(result["leads"])

                tool_results.append({
                    "role":         "tool",
                    "tool_call_id": tc.id,
                    "content":      json.dumps(result),
                })

            messages.extend(tool_results)
        success = True

    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
    finally:
        _record_perf(provider, model,
                     int((_time.time() - t0) * 1000),
                     total_in, total_out, total_tools, total_leads, success)

    yield f"data: {json.dumps({'type': 'done'})}\n\n"


def run_agent_gemini(user_message, history, gemini_key, model,
                     apollo_key="", hubspot_token=""):
    yield from _run_agent_openai_compat(
        user_message, history,
        api_key=gemini_key,
        model=model,
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        provider="gemini",
        apollo_key=apollo_key,
        hubspot_token=hubspot_token,
    )


def run_agent_perplexity(user_message, history, perplexity_key, model,
                         apollo_key="", hubspot_token=""):
    """
    Perplexity sonar models have built-in web search but do NOT support
    function/tool calling — so we use a plain chat completion loop.
    """
    import time as _time
    from openai import OpenAI as _OAI

    client = _OAI(api_key=perplexity_key, base_url="https://api.perplexity.ai/")

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        role    = msg.get("role")
        content = msg.get("content")
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_message})

    t0 = _time.time()
    success = False
    input_tokens = output_tokens = 0
    try:
        response = client.chat.completions.create(model=model, messages=messages)
        usage = getattr(response, "usage", None)
        if usage:
            input_tokens  = getattr(usage, "prompt_tokens",     0) or 0
            output_tokens = getattr(usage, "completion_tokens", 0) or 0
        text = response.choices[0].message.content or ""
        if text:
            yield f"data: {json.dumps({'type': 'text', 'content': text})}\n\n"
        success = True
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
    finally:
        _record_perf("perplexity", model,
                     int((_time.time() - t0) * 1000),
                     input_tokens, output_tokens, 0, 0, success)

    yield f"data: {json.dumps({'type': 'done'})}\n\n"


# ── Anthropic agent loop ──────────────────────────────────────────────────────

def run_agent_anthropic(user_message: str, history: list,
                        anthropic_key: str,
                        model="claude-opus-4-6",
                        apollo_key="", hubspot_token=""):
    """Agent loop using the Anthropic SDK."""
    import time as _time
    client = anthropic.Anthropic(api_key=anthropic_key)
    MODEL  = model or "claude-opus-4-6"

    api_messages = []
    for msg in history:
        role    = msg.get("role")
        content = msg.get("content")
        if role in ("user", "assistant") and content:
            api_messages.append({"role": role, "content": content})
    api_messages.append({"role": "user", "content": user_message})

    t0 = _time.time()
    success = False
    total_in = total_out = total_tools = total_leads = 0
    try:
        while True:
            response = client.messages.create(
                model=MODEL, max_tokens=4096,
                system=SYSTEM_PROMPT, tools=TOOLS,
                messages=api_messages,
            )
            total_in  += getattr(response.usage, "input_tokens",  0) or 0
            total_out += getattr(response.usage, "output_tokens", 0) or 0

            full_text  = ""
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
            total_tools += len(tool_calls)

            tool_results = []
            for tc in tool_calls:
                yield f"data: {json.dumps({'type': 'tool_start', 'name': tc.name})}\n\n"
                result = run_tool(tc.name, tc.input,
                                  apollo_key=apollo_key,
                                  hubspot_token=hubspot_token)
                yield f"data: {json.dumps({'type': 'tool_end', 'name': tc.name, 'result': result})}\n\n"
                if isinstance(result, dict) and result.get("leads"):
                    total_leads += len(result["leads"])
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": tc.id,
                    "content":     json.dumps(result),
                })

            api_messages.append({"role": "user", "content": tool_results})
        success = True

    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
    finally:
        _record_perf("anthropic", MODEL,
                     int((_time.time() - t0) * 1000),
                     total_in, total_out, total_tools, total_leads, success)

    yield f"data: {json.dumps({'type': 'done'})}\n\n"


# ── Unified entry point ───────────────────────────────────────────────────────

def run_agent(user_message: str, history: list,
              anthropic_key="", apollo_key="", hubspot_token="",
              claude_model="claude-opus-4-6",
              gemini_key="", model_provider="anthropic",
              gemini_model="gemini-3-flash-preview",
              perplexity_key="", perplexity_model="sonar-pro"):
    """Route to the right model provider based on settings."""

    if model_provider == "gemini":
        gemini_key = gemini_key or os.getenv("GEMINI_API_KEY", "")
        if not gemini_key:
            yield f"data: {json.dumps({'type': 'text', 'content': 'Please configure your Gemini API key in Settings.'})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            return
        yield from run_agent_gemini(user_message, history,
                                    gemini_key=gemini_key,
                                    model=gemini_model,
                                    apollo_key=apollo_key,
                                    hubspot_token=hubspot_token)
        return

    if model_provider == "perplexity":
        perplexity_key = perplexity_key or os.getenv("PERPLEXITY_API_KEY", "")
        if not perplexity_key:
            yield f"data: {json.dumps({'type': 'text', 'content': 'Please configure your Perplexity API key in Settings.'})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            return
        yield from run_agent_perplexity(user_message, history,
                                        perplexity_key=perplexity_key,
                                        model=perplexity_model,
                                        apollo_key=apollo_key,
                                        hubspot_token=hubspot_token)
        return

    # Default: Anthropic
    anthropic_key = anthropic_key or os.getenv("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        yield f"data: {json.dumps({'type': 'text', 'content': 'Please configure your Anthropic API key in Settings.'})}\n\n"
        yield f"data: {json.dumps({'type': 'done'})}\n\n"
        return
    yield from run_agent_anthropic(user_message, history,
                                   anthropic_key=anthropic_key,
                                   model=claude_model,
                                   apollo_key=apollo_key,
                                   hubspot_token=hubspot_token)


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({
        "anthropic":      bool(session.get("anthropic_key")  or os.getenv("ANTHROPIC_API_KEY")),
        "apollo":         bool(session.get("apollo_key")     or os.getenv("APOLLO_API_KEY")),
        "hubspot":        bool(session.get("hubspot_token")  or os.getenv("HUBSPOT_TOKEN")),
        "gemini":         bool(session.get("gemini_key")       or os.getenv("GEMINI_API_KEY")),
        "perplexity":     bool(session.get("perplexity_key")   or os.getenv("PERPLEXITY_API_KEY")),
        "model_provider":   session.get("model_provider",   "anthropic"),
        "claude_model":     session.get("claude_model",     "claude-opus-4-6"),
        "gemini_model":     session.get("gemini_model",     "gemini-3-flash-preview"),
        "perplexity_model": session.get("perplexity_model", "sonar-pro"),
    })


@app.route("/api/config", methods=["POST"])
def save_config():
    data = request.get_json(force=True)
    if data.get("anthropic_key"):
        session["anthropic_key"]  = data["anthropic_key"]
    if data.get("apollo_key"):
        session["apollo_key"]     = data["apollo_key"]
    if data.get("hubspot_token"):
        session["hubspot_token"]  = data["hubspot_token"]
    if data.get("gemini_key"):
        session["gemini_key"]       = data["gemini_key"]
    if data.get("model_provider"):
        session["model_provider"]   = data["model_provider"]
    if data.get("claude_model"):
        session["claude_model"]     = data["claude_model"]
    if data.get("gemini_model"):
        session["gemini_model"]     = data["gemini_model"]
    if data.get("perplexity_key"):
        session["perplexity_key"]   = data["perplexity_key"]
    if data.get("perplexity_model"):
        session["perplexity_model"] = data["perplexity_model"]
    return jsonify({"ok": True})


@app.route("/api/chat", methods=["POST"])
def chat():
    data    = request.get_json(force=True)
    message = data.get("message", "")
    history = data.get("history", [])

    # Read session BEFORE entering the streaming generator
    anthropic_key  = session.get("anthropic_key")  or os.getenv("ANTHROPIC_API_KEY", "")
    apollo_key     = session.get("apollo_key")     or os.getenv("APOLLO_API_KEY", "")
    hubspot_token  = session.get("hubspot_token")  or os.getenv("HUBSPOT_TOKEN", "")
    gemini_key       = session.get("gemini_key")       or os.getenv("GEMINI_API_KEY", "")
    model_provider   = session.get("model_provider",   "anthropic")
    claude_model     = session.get("claude_model",     "claude-opus-4-6")
    gemini_model     = session.get("gemini_model",     "gemini-2.0-flash")
    perplexity_key   = session.get("perplexity_key")   or os.getenv("PERPLEXITY_API_KEY", "")
    perplexity_model = session.get("perplexity_model", "sonar-pro")

    def stream():
        try:
            yield from run_agent(message, history,
                                 anthropic_key=anthropic_key,
                                 apollo_key=apollo_key,
                                 hubspot_token=hubspot_token,
                                 claude_model=claude_model,
                                 gemini_key=gemini_key,
                                 model_provider=model_provider,
                                 gemini_model=gemini_model,
                                 perplexity_key=perplexity_key,
                                 perplexity_model=perplexity_model)
        except Exception as e:
            app.logger.error("Unhandled stream error [%s]: %s", model_provider, e, exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/performance")
def get_performance():
    """Return aggregated and raw performance stats."""
    from collections import defaultdict

    records = list(_perf_store)  # snapshot

    # Per-provider aggregates
    agg = defaultdict(lambda: {
        "requests": 0, "successes": 0,
        "total_ms": 0, "total_in": 0, "total_out": 0,
        "total_tools": 0, "total_leads": 0, "total_cost": 0.0,
    })
    for r in records:
        p = r["provider"]
        agg[p]["requests"]    += 1
        agg[p]["successes"]   += 1 if r["success"] else 0
        agg[p]["total_ms"]    += r["duration_ms"]
        agg[p]["total_in"]    += r["input_tokens"]
        agg[p]["total_out"]   += r["output_tokens"]
        agg[p]["total_tools"] += r["tool_calls"]
        agg[p]["total_leads"] += r["leads_found"]
        agg[p]["total_cost"]  += r["cost_usd"]

    summary = {}
    for p, d in agg.items():
        n = d["requests"]
        summary[p] = {
            "requests":       n,
            "success_rate":   round(d["successes"] / n * 100, 1) if n else 0,
            "avg_ms":         round(d["total_ms"] / n) if n else 0,
            "total_tokens":   d["total_in"] + d["total_out"],
            "avg_tokens":     round((d["total_in"] + d["total_out"]) / n) if n else 0,
            "total_leads":    d["total_leads"],
            "avg_leads":      round(d["total_leads"] / n, 1) if n else 0,
            "total_cost_usd": round(d["total_cost"], 4),
            "avg_cost_usd":   round(d["total_cost"] / n, 4) if n else 0,
        }

    # Last 20 raw records (newest first)
    recent = sorted(records, key=lambda r: r["ts"], reverse=True)[:20]

    return jsonify({"summary": summary, "recent": recent})


@app.route("/api/download/leads")
def download_leads():
    path = os.path.join(os.path.dirname(__file__), "leads.csv")
    if os.path.exists(path):
        return send_file(path, mimetype="text/csv",
                         as_attachment=True, download_name="leads.csv")
    global _leads_store
    if not _leads_store:
        return jsonify({"error": "No leads available"}), 404
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=LEAD_FIELDS, extrasaction="ignore")
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
    writer = csv.DictWriter(
        output, fieldnames=["name", "email", "subject_line", "email_body"],
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(_outreach_store)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=outreach_drafts.csv"},
    )


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    app.run(port=8501, debug=True, use_reloader=False)
