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
_leads_store = []
_outreach_store = []

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
    "trade_name",
    "entity_name",
    "formation_date",
    "years_in_business",
    "general_email",
    "owner_name",
    "owner_email",
    "owner_phone",
    "registered_agent",
    "reg_agent_address",
    "business_phone",
    "address",
    "website",
    "instagram_url",
    "facebook_url",
    "google_review_count",
    "google_rating",
    "industry",
    "employees",
    "linkedin_url",
    "sunbiz_url",
    "sunbiz_status",
]


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
            "Returns trade name, website, phone, LinkedIn, Facebook, address, industry, "
            "founded year. Use this as the first step to get a list of businesses."
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
            "Enrich a list of leads all at once with Sunbiz corporate data, "
            "website contact info (email, Instagram, Facebook), and Google Maps reviews. "
            "ALWAYS use this instead of calling sunbiz_lookup / scrape_website_contact / "
            "get_google_reviews individually — it does all three in one call for every lead, "
            "saving tokens and time. Pass the full leads list from apollo_search_people."
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

            # Pick the best-matching result by name overlap
            search_norm = re.sub(r"[^a-z0-9]", "", business_name.lower())
            best_href, best_score = result_pairs[0][0], -1
            for href, name in result_pairs:
                name_norm = re.sub(r"[^a-z0-9]", "", name.lower().replace("&amp;", ""))
                score = sum(1 for c in search_norm if c in name_norm)
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

        # Extract from aria-label attributes set by Google Maps
        # Rating: aria-label="4.3 stars"
        rating_m = re.search(
            r'aria-label="(\d\.\d)\s*stars?"', html, re.IGNORECASE
        )
        # Count: aria-label="273 reviews"
        count_m = re.search(
            r'aria-label="([\d,]+)\s*reviews?"', html, re.IGNORECASE
        )

        if rating_m:
            rating = rating_m.group(1)
        if count_m:
            count = count_m.group(1).replace(",", "")

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
        name  = result.get("trade_name", "")
        url   = result.get("website", "")
        city  = result.get("city", "")
        state = result.get("state", "")

        # 1. Sunbiz
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

        # 2. Website scrape
        if url:
            try:
                ws = scrape_website_contact(url)
                result["general_email"] = ws.get("general_email", "")
                if ws.get("instagram_url"):
                    result["instagram_url"] = ws["instagram_url"]
                if ws.get("facebook_url") and not result.get("facebook_url"):
                    result["facebook_url"] = ws["facebook_url"]
            except Exception:
                pass

        # 3. Google reviews
        try:
            gr = get_google_reviews(name, city, state)
            result["google_rating"]       = gr.get("google_rating", "")
            result["google_review_count"] = gr.get("google_review_count", "")
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
        "enriched_leads": enriched,
        "total": len(enriched),
        "saved": True,
    }


TOOL_MAP = {
    "web_search":             web_search,
    "apollo_search_people":   apollo_search_people,
    "enrich_leads_batch":     enrich_leads_batch,
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

## Workflow

**Step 1 — Search Apollo**
Call apollo_search_people with the keyword and location. This returns a list of businesses.

**Step 2 — Enrich all leads in ONE call**
Pass the FULL leads list to enrich_leads_batch. This single tool call handles Sunbiz,
website scraping, and Google reviews for every lead simultaneously — do NOT call
sunbiz_lookup, scrape_website_contact, or get_google_reviews individually.

**Step 3 — Report results**
Summarize the enriched leads: show a table with trade name, entity name, years in business,
owner, registered agent, email, Instagram, Facebook, Google rating and review count.

**Step 4 — Optional next steps**
- Use hubspot_create_contact to push leads to HubSpot CRM
- Draft outreach emails and call save_outreach_csv

## Rules
- NEVER call sunbiz_lookup, scrape_website_contact, or get_google_reviews one-by-one.
  Always use enrich_leads_batch for enrichment.
- Do not use web_search unless the user explicitly asks you to look something up.
- When a field can't be found, leave it blank — never guess.
"""


def run_agent(user_message: str, history: list,
              anthropic_key="", apollo_key="", hubspot_token=""):
    """Generator that yields SSE-formatted strings."""
    anthropic_key = anthropic_key or os.getenv("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        yield f"data: {json.dumps({'type': 'text', 'content': 'Please configure your Anthropic API key in Settings.'})}\n\n"
        yield f"data: {json.dumps({'type': 'done'})}\n\n"
        return

    client = anthropic.Anthropic(api_key=anthropic_key)

    api_messages = []
    for msg in history:
        role    = msg.get("role")
        content = msg.get("content")
        if role in ("user", "assistant") and content:
            api_messages.append({"role": role, "content": content})
    api_messages.append({"role": "user", "content": user_message})

    try:
        while True:
            response = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=api_messages,
            )

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

            tool_results = []
            for tc in tool_calls:
                yield f"data: {json.dumps({'type': 'tool_start', 'name': tc.name})}\n\n"
                result = run_tool(tc.name, tc.input,
                                  apollo_key=apollo_key,
                                  hubspot_token=hubspot_token)
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
    data    = request.get_json(force=True)
    message = data.get("message", "")
    history = data.get("history", [])

    # Read session BEFORE entering the streaming generator
    anthropic_key = session.get("anthropic_key") or os.getenv("ANTHROPIC_API_KEY", "")
    apollo_key    = session.get("apollo_key")     or os.getenv("APOLLO_API_KEY", "")
    hubspot_token = session.get("hubspot_token")  or os.getenv("HUBSPOT_TOKEN", "")

    def stream():
        yield from run_agent(message, history,
                             anthropic_key=anthropic_key,
                             apollo_key=apollo_key,
                             hubspot_token=hubspot_token)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
    app.run(port=8501, debug=False)
