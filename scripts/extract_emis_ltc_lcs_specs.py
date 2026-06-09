"""Extract LTC LCS risk stratification specs from the EMIS XML agent API.

Loads the shared R5 export from blob storage, filters reports to the
Risk Stratification R2 folder, and writes one markdown implementation
guide per report plus an INDEX.md.

Usage:
    python scripts/extract_emis_ltc_lcs_specs.py
    python scripts/extract_emis_ltc_lcs_specs.py --pathname "xml-files/NCL LTC LCS R5 updated 27112025.xml"
"""

import argparse
import json
import re
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

BASE_URL = "https://emis-xml-to-snomed.vercel.app"
DEFAULT_PATHNAME = "xml-files/NCL LTC LCS R5 updated 27112025.xml"
DEFAULT_FOLDER_FILTER = "Risk Stratification R2"
DEFAULT_OUT_DIR = "docs/emis_specs/ltc_lcs_rs"


def api(path, body=None, timeout=120):
    url = f"{BASE_URL}{path}"
    if body is not None:
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    else:
        req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def slugify(name):
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
    return re.sub(r"_+", "_", slug)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pathname", default=DEFAULT_PATHNAME, help="Blob pathname of the EMIS XML export")
    parser.add_argument("--folder-filter", default=DEFAULT_FOLDER_FILTER, help="Substring of folderPath to include")
    parser.add_argument("--out", default=DEFAULT_OUT_DIR, help="Output directory for markdown files")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading document from blob: {args.pathname}")
    doc = api("/api/agent/load-xml-from-blob", {"pathname": args.pathname})["document"]
    doc_id = doc["id"]
    print(f"Document {doc_id} ({doc['reportCount']} reports, sha {doc['xmlSha256'][:12]})")

    reports = api(f"/api/agent/documents/{doc_id}/reports")["reports"]
    targets = [r for r in reports if args.folder_filter in (r.get("folderPath") or "")]
    print(f"{len(targets)} reports under '{args.folder_filter}'")

    # Identical searches can appear more than once in the export; keep one per name.
    seen, deduped, duplicates = {}, [], []
    for r in sorted(targets, key=lambda r: (r["searchName"], r["id"])):
        key = r["searchName"]
        if key in seen:
            duplicates.append((r["id"], key, seen[key]))
        else:
            seen[key] = r["id"]
            deduped.append(r)
    print(f"{len(deduped)} unique search names ({len(duplicates)} duplicates skipped)")

    index_rows = []
    for i, r in enumerate(deduped, 1):
        slug = slugify(r["searchName"])
        fname = f"{slug}.md"
        print(f"[{i}/{len(deduped)}] {r['searchName']} -> {fname}")
        for attempt in range(3):
            try:
                resp = api("/api/agent/query", {
                    "documentId": doc_id,
                    "action": "getImplementationGuide",
                    "reportId": r["id"],
                })
                break
            except Exception as exc:
                if attempt == 2:
                    raise
                print(f"  retry after error: {exc}")
                time.sleep(2 * (attempt + 1))
        guide = resp["report"]["implementationGuideMarkdown"]
        header = (
            f"<!-- Extracted from '{doc['fileName']}' (sha256 {doc['xmlSha256']})\n"
            f"     report id: {r['id']}\n"
            f"     folder: {r.get('folderPath', '')}\n"
            f"     extracted: {date.today().isoformat()} by scripts/extract_emis_ltc_lcs_specs.py\n"
            f"     Readable guide only: for exact operators/ranges query the agent API\n"
            f"     (agentInterpretation.decisionFlow[].criteriaDetails). -->\n\n"
        )
        (out_dir / fname).write_text(header + guide, encoding="utf-8")
        index_rows.append((r["searchName"], fname, r["id"]))

    index = [
        "# LTC LCS Risk Stratification - EMIS R5 specs",
        "",
        f"Implementation guides extracted from `{doc['fileName']}`",
        f"(sha256 `{doc['xmlSha256']}`) on {date.today().isoformat()}.",
        "",
        "Regenerate with `python scripts/extract_emis_ltc_lcs_specs.py`.",
        "",
        "| Search | File |",
        "|---|---|",
    ]
    index += [f"| {name} | [{fname}]({fname}) |" for name, fname, _ in index_rows]
    if duplicates:
        index += ["", "## Skipped duplicates", "",
                  "Identical search names appearing more than once in the export:", ""]
        index += [f"- `{name}` ({rid}, kept {kept})" for rid, name, kept in duplicates]
    (out_dir / "INDEX.md").write_text("\n".join(index) + "\n", encoding="utf-8")
    print(f"Wrote {len(index_rows)} guides + INDEX.md to {out_dir}")


if __name__ == "__main__":
    sys.exit(main())
