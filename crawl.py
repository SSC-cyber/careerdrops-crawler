#!/usr/bin/env python3
"""CareerDrops crawler.

Reads public job feeds of company career pages (Greenhouse, Lever, Ashby),
normalises the jobs and posts them in batches to the CareerDrops ingest endpoint.

Usage examples:
    python crawl.py --limit 200 --dry-run       # test, nothing is written
    python crawl.py --limit 200                  # real test run on 200 companies
    python crawl.py                              # everything in the company list

The ingest password comes from the environment variable INGEST_TOKEN (never hard-code it).
"""
import argparse
import csv
import html
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

UA = "CareerDropsBot/1.0 (+https://careerdrops.com; contact@careerdrops.com)"
DEFAULT_INGEST_URL = "https://careerdrops.com/api/public/ingest-jobs"
MAX_AGE_DAYS = 180  # skip postings older than this (the site also filters by age)
MAX_JOBS_PER_COMPANY = 500  # the ingest endpoint expires jobs missing from a request, so one request per company
HERE = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def strip_html(text):
    """Greenhouse returns HTML that is itself HTML-escaped, so unescape twice."""
    if not text:
        return ""
    s = html.unescape(html.unescape(str(text)))
    s = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<br\s*/?>|</p>|</li>|</div>|</h\d>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def iso_from_ms(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def to_int(v):
    try:
        if v is None or v == "":
            return None
        return int(round(float(v)))
    except Exception:
        return None


def annualise(value, interval):
    """Convert an hourly/monthly figure to a yearly one. Unknown intervals are treated as yearly."""
    v = to_int(value)
    if v is None:
        return None
    i = (interval or "").lower()
    if "hour" in i:
        return v * 2080
    if "month" in i:
        return v * 12
    if "week" in i:
        return v * 52
    return v


def is_remote_text(*parts):
    t = " ".join(p for p in parts if p).lower()
    return "remote" in t


# ----------------------------------------------------------------------------
# per-ATS fetch + normalise
# ----------------------------------------------------------------------------

def fetch_json(session, url, retries=2, timeout=25):
    """Returns (status_code, data). data is None for 404 or when all retries fail."""
    last = None
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 404:
                return 404, None
            if r.status_code == 429 or r.status_code >= 500:
                last = r.status_code
                time.sleep(2 * (attempt + 1) + random.random())
                continue
            if r.status_code != 200:
                return r.status_code, None
            return 200, r.json()
        except Exception as exc:  # network error, bad JSON...
            last = str(exc)[:80]
            time.sleep(1 + attempt + random.random())
    return (last if isinstance(last, int) else 0), None


def norm_greenhouse(job, slug, company):
    loc = (job.get("location") or {}).get("name") or ""
    smin = smax = cur = None
    for rng in job.get("pay_input_ranges") or []:
        mn, mx = rng.get("min_cents"), rng.get("max_cents")
        if mn is not None:
            smin = to_int(mn) // 100 if to_int(mn) is not None else None
        if mx is not None:
            smax = to_int(mx) // 100 if to_int(mx) is not None else None
        cur = rng.get("currency_type") or cur
        break
    return {
        "external_id": str(job.get("id")),
        "title": (job.get("title") or "").strip(),
        "location": loc,
        "country": None,
        "remote": is_remote_text(loc, job.get("title")),
        "salary_min": smin,
        "salary_max": smax,
        "currency": cur,
        "employment_type": None,
        "summary": strip_html(job.get("content"))[:500],
        "apply_url": job.get("absolute_url"),
        "posted_at": job.get("first_published") or job.get("updated_at"),
    }


def norm_lever(job, slug, company):
    cat = job.get("categories") or {}
    loc = cat.get("location") or ", ".join(cat.get("allLocations") or [])
    wt = (job.get("workplaceType") or "").lower()
    sr = job.get("salaryRange") or {}
    return {
        "external_id": str(job.get("id")),
        "title": (job.get("text") or "").strip(),
        "location": loc,
        "country": job.get("country"),
        "remote": True if wt == "remote" else (False if wt in ("on-site", "onsite", "hybrid") else is_remote_text(loc)),
        "salary_min": annualise(sr.get("min"), sr.get("interval")),
        "salary_max": annualise(sr.get("max"), sr.get("interval")),
        "currency": sr.get("currency"),
        "employment_type": cat.get("commitment"),
        "summary": strip_html(job.get("descriptionPlain") or job.get("description"))[:500],
        "apply_url": job.get("hostedUrl") or job.get("applyUrl"),
        "posted_at": iso_from_ms(job.get("createdAt")),
    }


def norm_ashby(job, slug, company):
    loc = job.get("location") or ""
    wt = (job.get("workplaceType") or "").lower()
    remote = job.get("isRemote")
    if remote is None:
        remote = True if wt == "remote" else (False if wt in ("onsite", "hybrid") else is_remote_text(loc))
    smin = smax = cur = None
    comp = job.get("compensation") or {}
    for c in comp.get("summaryComponents") or []:
        if (c.get("compensationType") or "").lower() == "salary":
            smin = annualise(c.get("minValue"), c.get("interval"))
            smax = annualise(c.get("maxValue"), c.get("interval"))
            cur = c.get("currencyCode")
            break
    return {
        "external_id": str(job.get("id")),
        "title": (job.get("title") or "").strip(),
        "location": loc,
        "country": None,
        "remote": bool(remote),
        "salary_min": smin,
        "salary_max": smax,
        "currency": cur,
        "employment_type": job.get("employmentType"),
        "summary": strip_html(job.get("descriptionPlain") or job.get("descriptionHtml"))[:500],
        "apply_url": job.get("jobUrl") or job.get("applyUrl"),
        "posted_at": job.get("publishedAt"),
    }


ATS = {
    "greenhouse": {
        "url": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
        "extract": lambda d: d.get("jobs") if isinstance(d, dict) else None,
        "norm": norm_greenhouse,
    },
    "lever": {
        "url": "https://api.lever.co/v0/postings/{slug}?mode=json",
        "extract": lambda d: d if isinstance(d, list) else None,
        "norm": norm_lever,
    },
    "ashby": {
        "url": "https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true",
        "extract": lambda d: [j for j in (d.get("jobs") or []) if j.get("isListed", True)] if isinstance(d, dict) else None,
        "norm": norm_ashby,
    },
}


# ----------------------------------------------------------------------------
# company list
# ----------------------------------------------------------------------------

def load_agency_words():
    path = os.path.join(HERE, "agency_names.txt")
    words = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip().lower()
                if line and not line.startswith("#"):
                    words.append(line)
    except FileNotFoundError:
        pass
    return words


def load_companies(path):
    """CSV with columns ats_vendor, company_name, board_slug (extra columns ignored)."""
    out, seen = [], set()
    with open(path, newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            ats = (row.get("ats_vendor") or row.get("ats") or "").strip().lower()
            slug = (row.get("board_slug") or row.get("slug") or "").strip()
            name = (row.get("company_name") or row.get("name") or slug).strip()
            if ats not in ATS or not slug:
                continue
            key = (ats, slug.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append({"ats": ats, "slug": slug, "name": name})
    return out


def pick_company_file(explicit):
    for p in [explicit, os.path.join(HERE, "data", "ats_directory.csv"), os.path.join(HERE, "data", "seed_companies.csv")]:
        if p and os.path.exists(p):
            return p
    return None


# ----------------------------------------------------------------------------
# posting to CareerDrops
# ----------------------------------------------------------------------------

def post_batch(session, url, token, payload, retries=2):
    last = None
    for attempt in range(retries + 1):
        try:
            r = session.post(url, json=payload, headers={"x-ingest-token": token}, timeout=90)
            if r.status_code == 200:
                return True, r.json()
            if r.status_code in (401, 403):
                return False, {"error": f"auth rejected ({r.status_code}) - check INGEST_TOKEN"}
            last = {"error": f"HTTP {r.status_code}: {r.text[:200]}"}
            if r.status_code < 500 and r.status_code != 429:
                return False, last
        except Exception as exc:
            last = {"error": str(exc)[:200]}
        time.sleep(2 * (attempt + 1))
    return False, last


SALES_INCLUDE = re.compile(
    r"\b(sales|account executive|account manager|account director|sdr|bdr|business development|"
    r"appointment setter|setter|closer|inside sales|outside sales|outbound|new business|"
    r"territory manager|revenue (?:executive|representative)|client partner|partnerships? (?:manager|executive))\b",
    re.I)
SALES_EXCLUDE = re.compile(
    r"\b(sales ?(?:operations|ops|enablement|analyst|analytics|systems|tax|support|admin|administrator|coordinator)|"
    r"salesforce|revops|revenue operations|data analyst|software engineer|developer)\b", re.I)
SALES_ONLY = False  # set by --sales-only


def is_sales(title):
    t = title or ""
    return bool(SALES_INCLUDE.search(t)) and not SALES_EXCLUDE.search(t)


def too_old(posted_at):
    if not posted_at:
        return False
    try:
        dt = datetime.fromisoformat(str(posted_at).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).days > MAX_AGE_DAYS
    except Exception:
        return False


def process_company(session, comp, ingest_url, token, dry_run, delay):
    cfg = ATS[comp["ats"]]
    time.sleep(random.random() * delay)
    status, data = fetch_json(session, cfg["url"].format(slug=comp["slug"]))
    res = {"company": comp["name"], "ats": comp["ats"], "slug": comp["slug"], "fetch_status": status,
           "found": 0, "sent": 0, "server": None, "error": None, "sample": None}
    if status == 404:
        res["error"] = "not found"
        return res
    raw = cfg["extract"](data) if data is not None else None
    if raw is None:
        res["error"] = f"fetch failed ({status})"
        return res
    jobs = []
    seen_ids = set()
    for j in raw:
        try:
            n = cfg["norm"](j, comp["slug"], comp["name"])
        except Exception:
            continue
        if not n.get("external_id") or n["external_id"] == "None" or not n.get("title") or not n.get("apply_url"):
            continue
        if too_old(n.get("posted_at")):
            continue
        n["external_id"] = str(n["external_id"])
        if n["external_id"] in seen_ids:  # server rejects a batch with repeated IDs
            continue
        seen_ids.add(n["external_id"])
        jobs.append(n)
    if SALES_ONLY:
        jobs = [j for j in jobs if is_sales(j.get("title"))]
    res["found"] = len(jobs)
    jobs = jobs[:MAX_JOBS_PER_COMPANY]
    if SALES_ONLY and not jobs:  # nothing to send for this company
        return res
    if jobs:
        res["sample"] = jobs[0]["title"]
    if dry_run:
        res["sent"] = len(jobs)
        return res
    payload = {"source_ats": comp["ats"], "company_slug": comp["slug"], "company_name": comp["name"], "jobs": jobs}
    ok, body = post_batch(session, ingest_url, token, payload)
    if ok:
        res["sent"] = len(jobs)
        res["server"] = body
    else:
        res["error"] = body.get("error")
        res["auth_failed"] = "auth rejected" in (res["error"] or "")
    return res


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", help="CSV of companies (ats_vendor,company_name,board_slug)")
    ap.add_argument("--limit", type=int, default=0, help="only crawl the first N companies (0 = all)")
    ap.add_argument("--offset", type=int, default=0, help="skip the first N companies (to crawl in chunks)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--delay", type=float, default=0.3, help="max random pause before each company (seconds)")
    ap.add_argument("--rotate", type=int, default=0, metavar="N",
                    help="crawl N companies starting at a position that moves forward every day (wraps around the list), "
                         "so the whole list is covered over a few days")
    ap.add_argument("--max-minutes", type=float, default=0, help="stop gracefully after this many minutes (0 = no limit)")
    ap.add_argument("--sales-only", action="store_true", help="keep only sales jobs (used for the Daily Sales Jobs site)")
    ap.add_argument("--dry-run", action="store_true", help="fetch and count, but do not send anything")
    ap.add_argument("--ingest-url", default=os.environ.get("INGEST_URL") or DEFAULT_INGEST_URL)
    args = ap.parse_args(argv)

    global SALES_ONLY
    SALES_ONLY = args.sales_only
    token = os.environ.get("INGEST_TOKEN", "").strip()
    if not args.dry_run and not token:
        print("ERROR: INGEST_TOKEN is not set. Add it as a GitHub secret, or use --dry-run.", file=sys.stderr)
        return 2
    if token and not (token.isascii() and token.isprintable()):
        bad = [f"U+{ord(c):04X}@{i}" for i, c in enumerate(token) if not (c.isascii() and c.isprintable())][:5]
        print(f"ERROR: INGEST_TOKEN contains characters that are not plain letters/numbers/symbols ({', '.join(bad)}). "
              "Delete the secret in GitHub and paste it again as plain text.", file=sys.stderr)
        return 2

    path = pick_company_file(args.companies)
    if not path:
        print("ERROR: no company list found. Put the LastRound CSV at data/ats_directory.csv", file=sys.stderr)
        return 2
    companies = load_companies(path)
    agency = load_agency_words()
    before = len(companies)
    companies = [c for c in companies if not any(w in c["name"].lower() for w in agency)]
    skipped_agencies = before - len(companies)
    if args.rotate:
        total = len(companies)
        start = (datetime.now(timezone.utc).toordinal() * args.rotate) % max(total, 1)
        companies = (companies + companies)[start:start + min(args.rotate, total)]
        print(f"Rotating slice: start position {start} of {total}", flush=True)
    else:
        companies = companies[args.offset:]
        if args.limit:
            companies = companies[: args.limit]
    started = time.time()
    deadline = started + args.max_minutes * 60 if args.max_minutes else None
    print(f"Company list: {path} | companies to crawl: {len(companies)} | agency names skipped: {skipped_agencies} | dry-run: {args.dry_run}", flush=True)

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})

    results, auth_failed, timed_out = [], False, False
    pool = ThreadPoolExecutor(max_workers=max(1, args.workers))
    futs = {pool.submit(process_company, session, c, args.ingest_url, token, args.dry_run, args.delay): c for c in companies}
    done = 0
    try:
        for fut in as_completed(futs, timeout=(max(1, deadline - time.time()) if deadline else None)):
            r = fut.result()
            results.append(r)
            done += 1
            if r.get("auth_failed"):
                auth_failed = True
                print("Stopping: the ingest endpoint rejected the password.", file=sys.stderr, flush=True)
                break
            if done % 100 == 0:
                mins = (time.time() - started) / 60
                bad = sum(1 for x in results if x["error"] and x["error"] != "not found")
                print(f"  ...{done}/{len(companies)} companies | {mins:.1f} min | failed so far: {bad}", flush=True)
    except Exception as exc:  # TimeoutError from the time budget
        timed_out = True
        print(f"Time budget reached after {done}/{len(companies)} companies - writing the summary.", flush=True)
    for f in futs:
        f.cancel()
    pool.shutdown(wait=False, cancel_futures=True)

    # summary
    ok = [r for r in results if not r["error"]]
    notfound = [r for r in results if r["error"] == "not found"]
    failed = [r for r in results if r["error"] and r["error"] != "not found"]
    found = sum(r["found"] for r in results)
    sent = sum(r["sent"] for r in results)
    totals = {"inserted": 0, "updated": 0, "expired": 0, "duplicates": 0, "filtered": 0, "skipped": 0}
    for r in results:
        if isinstance(r.get("server"), dict):
            for k in totals:
                totals[k] += int(r["server"].get(k) or 0)
    by_ats = {}
    for r in results:
        a = by_ats.setdefault(r["ats"], {"companies": 0, "ok": 0, "jobs": 0})
        a["companies"] += 1
        a["ok"] += 0 if r["error"] else 1
        a["jobs"] += r["found"]

    lines = [
        "## CareerDrops crawl summary",
        f"- Mode: {'DRY RUN (nothing was sent)' if args.dry_run else 'LIVE'}",
        f"- Companies crawled: {len(results)} (ok {len(ok)}, not found {len(notfound)}, failed {len(failed)})",
        f"- Jobs found: {found} | jobs sent: {sent}",
    ]
    if not args.dry_run:
        lines.append("- Server counts: " + ", ".join(f"{k} {v}" for k, v in totals.items()))
    for a, v in sorted(by_ats.items()):
        lines.append(f"- {a}: {v['ok']}/{v['companies']} companies ok, {v['jobs']} jobs")
    if timed_out:
        lines.append(f"- STOPPED EARLY: time budget reached, {len(companies) - len(results)} companies were not reached")
    samples = [f"{r['company']}: {r['sample']}" for r in ok if r.get("sample")][:5]
    if samples:
        lines.append("- Sample jobs: " + " | ".join(samples))
    errs = {}
    for r in failed:
        errs[r["error"]] = errs.get(r["error"], 0) + 1
    if errs:
        lines.append("- Errors: " + ", ".join(f"{k} x{v}" for k, v in errs.items()))
    text = "\n".join(lines)
    print("\n" + text)
    summ = os.environ.get("GITHUB_STEP_SUMMARY")
    if summ:
        with open(summ, "a", encoding="utf-8") as fh:
            fh.write(text + "\n")
    if timed_out:  # stuck worker threads would otherwise keep the process alive
        sys.stdout.flush()
        os._exit(1 if auth_failed else 0)
    return 1 if auth_failed else 0


if __name__ == "__main__":
    sys.exit(main())
