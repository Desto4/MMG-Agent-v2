#!/usr/bin/env python3
"""
Import Florida DBPR cosmetology licensee data into Postgres `leads_db`.

Uses the public bulk file published at:
  https://www2.myfloridalicense.com/cosmetology/public-records
  → Cosmetology licensee file (e.g. COSMETOLOGYLICENSE_1.csv)

Read DBPR's ReadMe/Disclaimer and field docs before using in production.
Default data URL (same as linked from the public-records page):
  https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv

Requires:
  - DATABASE_URL in the environment (Postgres, sslmode often required on cloud hosts)
  - pip: psycopg2-binary (see requirements.txt)

Usage:
  export DATABASE_URL="postgresql://..."
  export DBPR_COSMO_CSV="/path/local.csv"   # optional; else download
  python3 import_dbpr_cosmetology.py

  # All licensed cosmetology *salons* in Miami-Dade (class CE, active, county 23):
  python3 import_dbpr_cosmetology.py --county 23

  # Subset: rows whose name/address look like *nail* businesses (default):
  python3 import_dbpr_cosmetology.py --county 23 --match nail

  # All CE salons in that county (no keyword filter):
  python3 import_dbpr_cosmetology.py --county 23 --match all

  # Dry run (no DB writes, print counts only):
  python3 import_dbpr_cosmetology.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone
from typing import Iterator, List, Optional, Tuple

# Official weekly extract; override with DBPR_COSMO_CSV or --url
DEFAULT_CSV_URL = os.environ.get(
    "DBPR_COSMO_URL",
    "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv",
)

# DBPR "County Code" in the cosmetology file (e.g. Dade = 23, Broward = 11).
# Not the same as FIPS; see DBPR code tables in their public ReadMe.
MIAMI_DADE_COUNTY = "23"

# Board column 0 is typically "05" (Cosmetology) for these rows; we do not require it strictly.

# Class: CE = cosmetology salon (facility). See DBPR "Understanding DBPR Codes".
CLASS_COSMETOLOGY_SALON = "CE"

# Secondary status: A = active (for rows in the active file still double-check)
STATUS_ACTIVE = "A"

# Keywords for --match nail (name, DBA, address)
_NAIL_PATTERNS = re.compile(
    r"\b(nail|nails|manicure|manicur|pedicure|pedicur|acrylics?|gels?|"
    r"dip powder|dipping|lash extension|nail bar|nail studio|nail spa)\b",
    re.I,
)


def _pg_url(require_ssl: bool = True) -> str:
    """
    Build a psycopg2 DSN. Fixes copy/paste issues: extra parens, ppostgresql typo, extra quotes.
    """
    u = (os.environ.get("DATABASE_URL") or "").strip()
    if not u:
        print("ERROR: Set DATABASE_URL to your Postgres connection string.", file=sys.stderr)
        sys.exit(1)
    if (u.startswith("(") and u.endswith(")")) or (u.startswith('"') and u.endswith('"')) or (
        u.startswith("'") and u.endswith("'")
    ):
        u = u[1:-1].strip()
    u_lower = u.lower()
    if u_lower.startswith("ppostgresql://"):
        u = "postgresql://" + u[len("ppostgresql://") :]
    elif u_lower.startswith("postgresql://") or u_lower.startswith("postgres://"):
        pass
    else:
        print(
            "ERROR: DATABASE_URL should start with postgresql:// (or postgres://).\n"
            "  No parentheses — copy the URL exactly from Render.\n"
            f"  First 40 characters look like: {u[:40]!r}…",
            file=sys.stderr,
        )
        sys.exit(1)
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    parts = urlsplit(u)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    if require_ssl and "sslmode" not in q and "ssl" not in (parts.netloc or "").lower():
        q["sslmode"] = "require"
    query = urlencode(q)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _stream_csv_rows(path_or_url: str) -> Iterator[List[str]]:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        req = urllib.request.Request(path_or_url, headers={"User-Agent": "MMG-Agent-DBPR-import/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = io.TextIOWrapper(resp, encoding="utf-8", errors="replace", newline="")
            yield from csv.reader(body)
    else:
        with open(path_or_url, encoding="utf-8", errors="replace", newline="") as f:
            yield from csv.reader(f)


def _join_addr(a1: str, a2: str, a3: str) -> str:
    parts = [p.strip() for p in (a1, a2, a3) if p and p.strip()]
    return ", ".join(parts)


def _row_in_scope(
    row: List[str],
    county: str,
    class_code: str,
    only_active: bool,
) -> bool:
    if len(row) < 15:
        return False
    # Public layout: Board(0), Class(1), Licensee(2), DBA(3), Addr1-3(4-6), City(8), State(9), Zip(10), County(11)
    cty = (row[11] or "").strip()
    cls = (row[1] or "").strip()
    if cty != county or cls != class_code:
        return False
    if only_active and (row[14] or "").strip() != STATUS_ACTIVE:
        return False
    return True


def _nail_text_match(name: str, dba: str, addr: str) -> bool:
    blob = f"{name} {dba} {addr}"
    return bool(_NAIL_PATTERNS.search(blob))


def _load_rows(
    path_or_url: str,
    county: str,
    class_code: str,
    only_active: bool,
    match_mode: str,
) -> Tuple[int, int, List[Tuple[str, ...]]]:
    out: List[Tuple[str, ...]] = []
    scanned = 0
    for row in _stream_csv_rows(path_or_url):
        scanned += 1
        if not _row_in_scope(row, county, class_code, only_active):
            continue
        name = (row[2] or "").strip()
        dba = (row[3] or "").strip() or name
        addr = _join_addr(row[4] or "", row[5] or "", row[6] or "")
        city = (row[8] or "").strip()
        st = (row[9] or "").strip()
        zraw = (row[10] or "").strip()
        dig = re.sub(r"[^\d]", "", zraw)
        if len(dig) >= 9:
            z = f"{dig[:5]}-{dig[5:9]}"
        elif len(dig) == 5:
            z = dig
        else:
            z = zraw
        lic = (row[12] or "").strip()
        exp = (row[17] or "").strip() if len(row) > 17 else ""
        pstat = (row[13] or "").strip()
        sstat = (row[14] or "").strip()

        if match_mode == "nail" and not _nail_text_match(name, dba, addr):
            continue

        display = dba or name
        use_cat = "Nail Salon" if match_mode == "nail" else "Cosmetology Salon"

        full_addr = ", ".join(p for p in (addr, f"{city}, {st} {z}".strip()) if p)
        notes = f"DBPR | Lic {lic} | Exp {exp} | class {class_code} | pri {pstat} sec {sstat} | import {datetime.now(timezone.utc).date().isoformat()}"
        row_tuple = (
            f"DBPR Cosmetology County {county}",  # sheet
            use_cat,  # use_category
            display,  # trade_name
            display,  # business_name
            full_addr,  # business_address
            city,
            st,
            z,
            county,  # dbpr_county
            use_cat,  # industry
            "",  # business_phone
            "",  # general_email
            "",  # business_website
            lic,  # license_number
            class_code,  # license_class
            f"{pstat}{sstat}",  # license_status
            notes,
        )
        out.append(row_tuple)

    return scanned, len(out), out


_COLS = (
    "sheet",
    "use_category",
    "trade_name",
    "Business_Name",
    "business_address",
    "City",
    "State",
    "Zip",
    "dbpr_county",
    "industry",
    "business_phone",
    "general_email",
    "business_website",
    "license_number",
    "license_class",
    "license_status",
    "notes",
)


def _replace_leads_table(conn, rows: List[Tuple[str, ...]]) -> None:
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS "leads_db"')
        cols_ddl = ", ".join(f'"{c}" TEXT' for c in _COLS)
        cur.execute(f'CREATE TABLE "leads_db" ({cols_ddl})')
        if not rows:
            return
        ph = ", ".join(["%s"] * len(_COLS))
        from psycopg2.extras import execute_batch

        execute_batch(
            cur,
            f'INSERT INTO "leads_db" VALUES ({ph})',
            rows,
            page_size=500,
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Load DBPR cosmetology salon extract into Postgres leads_db")
    ap.add_argument("--file", help="Local CSV path (else DBPR_COSMO_CSV or download --url)")
    ap.add_argument("--url", default=DEFAULT_CSV_URL, help="Download URL for extract")
    ap.add_argument("--county", default=MIAMI_DADE_COUNTY, help='DBPR county code (Miami-Dade = "23")')
    ap.add_argument("--class-code", default=CLASS_COSMETOLOGY_SALON, help="License class, default CE = cosmetology salon")
    ap.add_argument(
        "--match",
        choices=("nail", "all"),
        default="nail",
        help="nail: only rows whose name/address match nail-salon terms; all: every CE active salon in county",
    )
    ap.add_argument("--include-inactive", action="store_true", help="Do not require secondary status A (not recommended)")
    ap.add_argument("--dry-run", action="store_true", help="Count only; do not write to Postgres")
    ap.add_argument("--no-ssl", action="store_true", help="Omit sslmode=require for local Postgres without SSL")
    args = ap.parse_args()

    src = args.file or os.environ.get("DBPR_COSMO_CSV", "").strip() or args.url
    if args.file and not os.path.isfile(args.file):
        print(f"ERROR: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    only_active = not args.include_inactive
    print(f"Source: {src}")
    print(f"Filters: county={args.county} class={args.class_code} active_secondary={only_active!r} match={args.match}")

    scanned, kept, rows = _load_rows(
        src,
        county=args.county,
        class_code=args.class_code,
        only_active=only_active,
        match_mode=args.match,
    )
    print(f"Scanned {scanned:,} CSV lines; {kept:,} rows after filters.")

    if args.dry_run:
        return

    if kept == 0:
        print("No rows to import; table not modified.")
        return

    import psycopg2

    try:
        url = _pg_url(require_ssl=not args.no_ssl)
    except SystemExit:
        raise
    except Exception as e:
        print(f"ERROR: invalid DATABASE_URL: {e}", file=sys.stderr)
        sys.exit(1)
    if args.no_ssl:
        from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

        parts = urlsplit(url)
        q = {k: v for k, v in dict(parse_qsl(parts.query, keep_blank_values=True)).items() if k != "sslmode"}
        url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))

    try:
        conn = psycopg2.connect(url)
    except psycopg2.Error as e:
        print(
            "ERROR: could not connect to Postgres. Check DATABASE_URL in Render, copy the full URL only,\n"
            "  with no ( ) around it, and the scheme must be postgresql:// (not ppostgresql).",
            file=sys.stderr,
        )
        print(f"  Details: {e}", file=sys.stderr)
        sys.exit(1)
    try:
        _replace_leads_table(conn, rows)
        conn.commit()
        print(f'OK — replaced "leads_db" with {kept:,} rows.')
    finally:
        conn.close()


if __name__ == "__main__":
    main()
