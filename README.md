# CareerDrops crawler

Reads the public job feeds of company career pages (Greenhouse, Lever, Ashby) and posts the jobs to the CareerDrops ingest endpoint. Every job links to the employer's own page.

## One-time setup
1. Put the company list at `data/ats_directory.csv` (download the free CSV from the LastRound AI ATS Company Directory on DataHub; columns `ats_vendor, company_name, board_slug`). Without it, the crawler uses the small `data/seed_companies.csv` for testing.
2. In this repo: Settings -> Secrets and variables -> Actions -> New repository secret. Name `INGEST_TOKEN`, value = the same password you put in Lovable.

## Run it
Actions tab -> "Crawl career pages" -> Run workflow.
- First run: leave `dry_run` ticked (nothing is sent), limit 200. Check the summary.
- Second run: untick `dry_run`, limit 200. Check the jobs appear in the CareerDrops admin.
- Full crawl: limit 0 (or crawl in chunks with `offset`). Then enable the daily schedule in `.github/workflows/crawl.yml`.

## Local test
`python tests/selftest.py` runs offline with fake data.

## Notes
- Company names containing words in `agency_names.txt` are skipped (staffing agencies).
- Max 500 jobs per company per run (the endpoint expires jobs missing from a request).
- Requests are polite: 8 at a time, short random pauses, a clear User-Agent.
- Job data belongs to each employer. Link out to the employer's page and remove a company on request.

## Credit
Company directory: LastRound AI ATS Company Directory (CC BY 4.0), https://datahub.io/lastroundai-hiring-data/lastroundai-hiring-data/ats-directory
