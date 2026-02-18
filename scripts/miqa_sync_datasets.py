#!/usr/bin/env python3
import csv
import json
import os
import sys
from typing import Any, Dict, List, Tuple

import requests


def err(msg: str, code: int = 2) -> "None":
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def coerce_value(raw: str) -> Any:
    s = (raw or "").strip()
    if s == "":
        return None

    low = s.lower()
    if low in ("true", "false"):
        return low == "true"

    # int?
    try:
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
    except Exception:
        pass

    # float?
    try:
        if any(ch in s for ch in (".", "e", "E")):
            return float(s)
    except Exception:
        pass

    return s


def read_csv_items(path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not os.path.exists(path):
        err(f"source_path not found: {path}")

    items: List[Dict[str, Any]] = []
    warnings: List[str] = []
    seen_names = set()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            err("CSV missing header row")
        if "name" not in reader.fieldnames:
            err("CSV must include a 'name' column")

        for row_idx, row in enumerate(reader, start=2):  # header ~ line 1
            name = (row.get("name") or "").strip()
            if not name:
                warnings.append(f"Line {row_idx}: missing name; skipped")
                continue
            if name in seen_names:
                err(f"Duplicate name '{name}' found (line {row_idx})")
            seen_names.add(name)

            item: Dict[str, Any] = {"name": name}
            for k, v in row.items():
                if k == "name":
                    continue
                cv = coerce_value(v)
                if cv is None:
                    continue
                item[k] = cv

            items.append(item)

    return items, warnings


def main() -> int:
    app_key = os.environ.get("MIQA_APP_KEY", "").strip()
    base_url = os.environ.get("MIQA_BASE_URL", "").strip()
    endpoint = os.environ.get("MIQA_ENDPOINT", "/api/batch_ds_upsert").strip()

    source_path = os.environ.get("MIQA_SOURCE_PATH", ".miqa/samples.csv").strip()
    source_format = os.environ.get("MIQA_SOURCE_FORMAT", "csv").strip().lower()

    pipeline_id = os.environ.get("MIQA_PIPELINE_ID", "").strip()
    org_id = os.environ.get("MIQA_ORG_ID", "").strip()

    timeout = int(os.environ.get("MIQA_TIMEOUT", "60"))
    dry_run = os.environ.get("MIQA_DRY_RUN", "").strip().lower() in ("1", "true", "yes")

    if not app_key:
        err("Missing MIQA_APP_KEY (secret: app_key)")
    if not base_url:
        err("Missing MIQA_BASE_URL (input: base_url)")
    if not pipeline_id:
        err("Missing MIQA_PIPELINE_ID (input: pipeline_id)")
    if source_format != "csv":
        err(f"Unsupported source_format '{source_format}' (supported: csv)")

    url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")

    params: Dict[str, str] = {"pipeline_id": pipeline_id}
    if org_id:
        params["org_id"] = org_id
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "app-key": app_key,
    }
    
    gh_run_id = os.environ.get("GITHUB_RUN_ID", "")
    gh_workflow = os.environ.get("GITHUB_WORKFLOW", "")
    gh_job = os.environ.get("GITHUB_JOB", "")
    gh_actor = os.environ.get("GITHUB_ACTOR", "")
    gh_ref = os.environ.get("GITHUB_REF", "")
    gh_sha = os.environ.get("GITHUB_SHA", "")
    
    if gh_run_id:
        headers["X-Miqa-GitHub-Run-Id"] = gh_run_id
    if gh_workflow:
        headers["X-Miqa-GitHub-Workflow"] = gh_workflow
    if gh_job:
        headers["X-Miqa-GitHub-Job"] = gh_job
    if gh_actor:
        headers["X-Miqa-GitHub-Actor"] = gh_actor
    if gh_ref:
        headers["X-Miqa-GitHub-Ref"] = gh_ref
    if gh_sha:
        headers["X-Miqa-GitHub-SHA"] = gh_sha


    wf_version = os.environ.get("MIQA_WORKFLOW_VERSION", "").strip()
    headers["User-Agent"] = f"miqa-github-workflows/{wf_version} (dataset-sync)" if wf_version else "miqa-github-workflows (dataset-sync)"
    
    items, warnings = read_csv_items(source_path)
    for w in warnings:
        print(f"WARN: {w}")

    payload = {"items": items}

    if dry_run:
        print("DRY RUN")
        print("POST", url)
        print("Params:", json.dumps(params, indent=2))
        print("Headers:", json.dumps({**headers, "app-key": "***"}, indent=2))
        print(f"Body: {{'items': [...]}}  (items={len(items)})")
        print("Body:", json.dumps(payload, indent=2))
        return 0

    try:
        resp = requests.post(url, params=params, headers=headers, json=payload, timeout=timeout)
    except requests.RequestException as e:
        err(f"Request failed: {e}", code=1)

    print(f"HTTP {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)

    return 0 if 200 <= resp.status_code < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())
