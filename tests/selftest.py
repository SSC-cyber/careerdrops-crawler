"""Offline self-test: fake career-page data + a fake ingest endpoint. Run: python tests/selftest.py"""
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import crawl  # noqa: E402

GH = {"jobs": [{"id": 111, "title": "Account Executive", "location": {"name": "Remote - US"},
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/111", "updated_at": "2026-11-01T10:00:00Z",
                "content": "&lt;p&gt;Sell &amp;amp; grow&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Quota&lt;/li&gt;&lt;/ul&gt;",
                "pay_input_ranges": [{"min_cents": 9000000, "max_cents": 14000000, "currency_type": "USD"}]},
               {"id": 112, "title": "", "absolute_url": "x"}]}
LV = [{"id": "abc", "text": "Support Specialist", "categories": {"location": "Berlin", "commitment": "Full-time"},
       "workplaceType": "hybrid", "hostedUrl": "https://jobs.lever.co/acme/abc", "createdAt": 1762000000000,
       "descriptionPlain": "Help customers.", "salaryRange": {"currency": "EUR", "interval": "per-hour-wage", "min": 20, "max": 30}}]
AS = {"jobs": [{"id": "x1", "title": "Designer", "location": "Lagos", "isRemote": True, "employmentType": "FullTime",
                "jobUrl": "https://jobs.ashbyhq.com/acme/x1", "publishedAt": "2026-10-20T00:00:00Z", "descriptionPlain": "Design things.",
                "compensation": {"summaryComponents": [{"compensationType": "Salary", "interval": "1 YEAR", "currencyCode": "USD", "minValue": 80000, "maxValue": 100000}]}},
               {"id": "x2", "title": "Hidden", "jobUrl": "u", "isListed": False}]}

received = []


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.headers.get("x-ingest-token") != "secret":
            self.send_response(401); self.end_headers(); return
        received.append(body)
        out = json.dumps({"inserted": len(body["jobs"]), "updated": 0, "expired": 0, "duplicates": 0, "filtered": 0, "skipped": 0}).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(out)

    def log_message(self, *a):
        pass


def main():
    # normalisers
    g = crawl.norm_greenhouse(GH["jobs"][0], "acme", "Acme")
    assert g["external_id"] == "111" and g["remote"] is True and g["salary_min"] == 90000 and g["salary_max"] == 140000, g
    assert g["summary"] == "Sell & grow Quota", g["summary"]
    l = crawl.norm_lever(LV[0], "acme", "Acme")
    assert l["remote"] is False and l["salary_min"] == 20 * 2080 and l["posted_at"].startswith("2025-"), l
    a = crawl.norm_ashby(AS["jobs"][0], "acme", "Acme")
    assert a["remote"] is True and a["salary_min"] == 80000 and a["employment_type"] == "FullTime", a

    # fake network + fake endpoint
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_port}/ingest"
    feeds = {"greenhouse": (200, GH), "lever": (200, LV), "ashby": (200, AS)}
    crawl.fetch_json = lambda session, u, **k: feeds[next(x for x in feeds if x in u)] if "missing" not in u else (404, None)

    import requests
    s = requests.Session()
    for ats in feeds:
        r = crawl.process_company(s, {"ats": ats, "slug": "acme", "name": "Acme"}, url, "secret", False, 0)
        assert not r["error"] and r["sent"] >= 1, r
    r = crawl.process_company(s, {"ats": "lever", "slug": "missing", "name": "Gone"}, url, "secret", False, 0)
    assert r["error"] == "not found", r
    r = crawl.process_company(s, {"ats": "lever", "slug": "acme", "name": "Acme"}, url, "WRONG", False, 0)
    assert r["auth_failed"], r
    r = crawl.process_company(s, {"ats": "ashby", "slug": "acme", "name": "Acme"}, url, "", True, 0)
    assert r["sent"] == 1 and r["server"] is None, r  # dry run, hidden job dropped
    assert len(received) == 3 and all(set(b) == {"source_ats", "company_slug", "company_name", "jobs"} for b in received)
    # greenhouse job without a title is dropped
    assert len(received[0]["jobs"]) == 1

    # CLI end-to-end on the seed list (dry run) using the fake feeds
    os.environ["INGEST_TOKEN"] = "secret"
    rc = crawl.main(["--limit", "5", "--dry-run", "--delay", "0"])
    assert rc == 0
    print("ALL SELF-TESTS PASSED")


if __name__ == "__main__":
    main()
