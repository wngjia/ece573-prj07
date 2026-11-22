#!/usr/bin/env python3
"""Compare REST acceptance and Cassandra projection for one Project 7 fault."""

import argparse
import concurrent.futures
import json
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JOB_ID = re.compile(r"[0-9a-f]{32}\Z")


class TrialError(Exception):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def progress(message):
    print(message, file=sys.stderr, flush=True)


def kubectl(*arguments, timeout=30, check=True):
    try:
        result = subprocess.run(["kubectl", *map(str, arguments)], capture_output=True,
                                text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise TrialError(f"kubectl {' '.join(map(str, arguments))}: {error}") from error
    if check and result.returncode:
        raise TrialError(f"kubectl {' '.join(map(str, arguments))}: {result.stderr.strip()}")
    return result


def http_json(url, timeout, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    request = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None, None
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = raw.decode(errors="replace")
        return error.code, body, None
    except (OSError, ValueError) as error:
        return None, None, f"{type(error).__name__}: {error}"


def check_health(url, timeout):
    for attempt in range(10):
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=timeout) as response:
                if response.status == 204:
                    return
        except OSError:
            pass
        if attempt < 9:
            time.sleep(2)
    raise TrialError(f"health check failed at {url}/healthz; run ./deploy_apps.sh")


def create_seller(login_url, timeout):
    seller = "resilience_" + secrets.token_hex(7)
    status, body, error = http_json(login_url + "/accounts", timeout, {
        "account_id": seller, "password": secrets.token_urlsafe(24)})
    if status != 201 or not isinstance(body, dict) or body.get("account_id") != seller:
        raise TrialError(f"create seller: HTTP {status}, error={error}, body={body}")
    return seller


def submit_listing(index, url, seller, prefix, timeout):
    title = f"{prefix}{index:04d}"
    status, body, error = http_json(url + "/listings", timeout, {
        "seller_id": seller, "title": title, "credit_cost": 1})
    job_id = body.get("job_id") if isinstance(body, dict) else None
    if status == 202 and (not isinstance(job_id, str) or not JOB_ID.fullmatch(job_id)):
        error = "HTTP 202 did not include a valid job_id"
    return {"index": index, "title": title, "http_status": status,
            "job_id": job_id if status == 202 and not error else None,
            "error": error, "response": body, "finished_at": utc_now()}


def submit_workload(args, seller, prefix, midway):
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = []
        for index in range(args.count):
            wait = started + index / args.rate - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            futures.append(pool.submit(submit_listing, index, args.url, seller, prefix,
                                       args.client_timeout))
            if index + 1 == max(1, args.count // 2):
                midway.set()
        return [future.result() for future in concurrent.futures.as_completed(futures)]


def inspect_postgres(seller):
    query = ("SELECT j.job_id, j.status, COALESCE(j.result->>'listing_id', ''), "
             "CASE WHEN o.id IS NULL THEN 'missing' WHEN o.delivered_at IS NULL "
             "THEN 'pending' ELSE 'delivered' END "
             "FROM job_outcomes j LEFT JOIN projection_outbox o ON o.job_id = j.job_id "
             f"WHERE j.actor_id = '{seller}' AND j.kind = 'listing'")
    try:
        result = kubectl("exec", "postgres-0", "--", "psql", "-A", "-F", "|", "-t",
                         "-U", "marketplace", "-d", "marketplace", "-c", query, timeout=10)
        jobs = {}
        for line in result.stdout.splitlines():
            parts = line.split("|")
            if len(parts) != 4 or not JOB_ID.fullmatch(parts[0]):
                raise TrialError(f"unexpected Postgres result: {line}")
            jobs[parts[0]] = {"status": parts[1], "listing_id": parts[2] or None,
                              "outbox": parts[3]}
        return {"jobs": jobs, "error": None}
    except TrialError as error:
        return {"jobs": None, "error": str(error)}


def inspect_cassandra(seller):
    query = ("SELECT listing_id FROM marketplace_history.listings_by_seller "
             f"WHERE seller_id = '{seller}';")
    try:
        result = kubectl("exec", "cassandra-0", "--", "cqlsh", "-e", query, timeout=15)
        rows = re.search(r"\((\d+) rows?\)\s*$", result.stdout)
        ids = sorted(line.strip() for line in result.stdout.splitlines()
                     if re.fullmatch(r"listing-\d+", line.strip()))
        if not rows or len(ids) != int(rows.group(1)):
            raise TrialError(f"unexpected Cassandra result: {result.stdout[:300]}")
        return {"listing_ids": ids, "error": None}
    except TrialError as error:
        return {"listing_ids": None, "error": str(error)}


def infrastructure():
    try:
        result = kubectl("get", "statefulset/postgres", "statefulset/kafka",
                         "statefulset/cassandra", "deployment/login-service",
                         "deployment/marketplace-service", "deployment/marketplace-worker",
                         "deployment/outbox-worker", "-o", "json", timeout=15)
        items = json.loads(result.stdout)["items"]
        ready = {item["metadata"]["name"]:
                 item.get("status", {}).get("readyReplicas", 0) >= item["spec"]["replicas"]
                 for item in items}
        return {"ready": ready, "error": None}
    except (TrialError, KeyError, ValueError) as error:
        return {"ready": None, "error": str(error)}


def snapshot(seller, requests):
    accepted = [item for item in requests if item["http_status"] == 202 and item["job_id"]]
    job_ids = [item["job_id"] for item in accepted]
    postgres = inspect_postgres(seller)
    cassandra = inspect_cassandra(seller)
    jobs = postgres["jobs"]
    visible = cassandra["listing_ids"]
    completed = None if jobs is None else sorted(j for j in jobs if jobs[j]["status"] == "completed")
    rejected = None if jobs is None else sorted(j for j in jobs if jobs[j]["status"] == "rejected")
    unresolved = None if jobs is None else sorted(set(job_ids) - set(completed) - set(rejected))
    missing = None if jobs is None or visible is None else sorted(
        j for j in completed if jobs[j]["listing_id"] not in visible)
    pending = None if jobs is None else sorted(j for j in completed if jobs[j]["outbox"] == "pending")
    missing_outbox = None if jobs is None else sorted(j for j in completed if jobs[j]["outbox"] == "missing")
    statuses = Counter(str(item["http_status"]) if item["http_status"] is not None
                       else "transport_unknown" for item in requests)
    return {"counts": {"sampled_requests": len(requests),
                       "accepted_202": len(accepted),
                       "http_failures": sum(item["http_status"] not in (None, 202)
                                            for item in requests),
                       "transport_unknown": statuses["transport_unknown"],
                       "processed": None if jobs is None else len(completed) + len(rejected),
                       "completed": None if jobs is None else len(completed),
                       "rejected": None if jobs is None else len(rejected),
                       "unresolved": None if jobs is None else len(unresolved),
                       "cassandra_listings": None if visible is None else len(visible),
                       "missing_cassandra_projections": None if missing is None else len(missing),
                       "pending_outbox": None if pending is None else len(pending),
                       "missing_outbox": None if missing_outbox is None else len(missing_outbox)},
            "rejected_jobs": rejected, "unresolved_jobs": unresolved,
            "missing_cassandra_projection_jobs": missing, "pending_outbox_jobs": pending,
            "missing_outbox_jobs": missing_outbox,
            "infrastructure": infrastructure()}


def settled(snapshot_value):
    return (snapshot_value["unresolved_jobs"] == [] and
            snapshot_value["rejected_jobs"] == [] and
            snapshot_value["missing_cassandra_projection_jobs"] == [] and
            snapshot_value["pending_outbox_jobs"] == [] and
            snapshot_value["missing_outbox_jobs"] == [] and
            snapshot_value["infrastructure"]["error"] is None and
            all(snapshot_value["infrastructure"]["ready"].values()))


def wait_for_recovery(args, seller, requests):
    deadline = time.monotonic() + args.recovery_timeout
    while True:
        state = snapshot(seller, requests)
        if settled(state) or time.monotonic() >= deadline:
            return state
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def run_trial(args, scenario, fault):
    progress(f"Starting {scenario}: {args.count} listings at {args.rate:g}/s")
    seller = create_seller(args.login_url, args.client_timeout)
    prefix = f"prj07-{scenario}-{secrets.token_hex(5)}-"
    existing = kubectl("get", "-f", fault, "-o", "name", "--ignore-not-found")
    if existing.stdout.strip():
        raise TrialError(f"fault in {fault} already exists; remove it before the trial")

    midway = threading.Event()
    finished = threading.Event()
    workload = {}

    def run_workload():
        try:
            workload["requests"] = submit_workload(args, seller, prefix, midway)
        except Exception as error:
            workload["error"] = error
        finally:
            finished.set()

    thread = threading.Thread(target=run_workload, daemon=True)
    applied = False
    fault_error = None
    thread.start()
    try:
        while not midway.wait(0.1):
            if finished.is_set():
                raise TrialError(f"workload ended before halfway: {workload.get('error')}")
        progress(f"{scenario}: applying {fault.name} after {max(1, args.count // 2)} submissions")
        kubectl("apply", "-f", fault)
        applied = True
        thread.join()
        if "error" in workload:
            raise TrialError(f"workload failed: {workload['error']}")
        requests = workload["requests"]
        checkpoint = snapshot(seller, requests)
    finally:
        if applied:
            try:
                kubectl("delete", "-f", fault, "--ignore-not-found", timeout=45)
            except TrialError as error:
                fault_error = str(error)
                progress(f"Could not remove {fault}: {fault_error}")
    after = wait_for_recovery(args, seller, requests)
    return {"checkpoint": checkpoint, "after_recovery": after,
            "fault_cleanup_error": fault_error}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fault", type=Path, help="path to one fault .yaml or .yml manifest")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=2.0, help="listing POSTs per second")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--client-timeout", type=float, default=30.0)
    parser.add_argument("--recovery-timeout", type=float, default=120.0)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--login-url", default="http://127.0.0.1:8081")
    parser.add_argument("--output", type=Path, help="JSON result path (default: out/resilience-TIMESTAMP.json)")
    args = parser.parse_args()
    if (args.count < 1 or args.rate <= 0 or args.concurrency < 1 or
            args.client_timeout <= 0 or args.recovery_timeout <= 0):
        parser.error("count, rate, concurrency, client timeout, and recovery timeout must be positive")
    if args.fault.suffix.lower() not in (".yaml", ".yml") or not args.fault.is_file():
        parser.error(f"fault must be an existing .yaml or .yml file: {args.fault}")
    args.url = args.url.rstrip("/")
    args.login_url = args.login_url.rstrip("/")
    kubectl("config", "use-context", "kind-ece573-prj07")
    initial = infrastructure()
    if initial["error"] or not all(initial["ready"].values()):
        raise TrialError(f"all project components must be ready: {initial}")
    check_health(args.url, min(args.client_timeout, 5))
    check_health(args.login_url, min(args.client_timeout, 5))
    output = args.output or ROOT / "out" / f"resilience-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    trial = run_trial(args, args.fault.stem, args.fault)
    stages = ("checkpoint", "after_recovery")
    report = {"fault": str(args.fault),
              **{stage: trial[stage]["counts"] for stage in stages}}
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Results saved to {output}")
    displayed_counts = ("accepted_202", "processed", "cassandra_listings", "pending_outbox")
    for stage in stages:
        counts = report[stage]
        print(f"{stage}: " + ", ".join(f"{name}={counts[name]}" for name in displayed_counts))
    after = trial["after_recovery"]
    if trial["fault_cleanup_error"]:
        progress(f"Fault cleanup failed: {trial['fault_cleanup_error']}")
    if not settled(after):
        progress(f"Recovery did not complete; inspect {output}")
    return 1 if trial["fault_cleanup_error"] or not settled(after) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (TrialError, KeyboardInterrupt) as error:
        print(f"run_resilience.py: {error or 'interrupted'}", file=sys.stderr)
        sys.exit(1)
