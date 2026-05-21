"""Erasmus+ parser. Reads wide-format CSVs from collector, unpivots partners,
filters Norwegian organisations, resolves orgnr via enheter name match, emits 12-col CDC.

CSV schema is wide: Coordinating organisation + Partner 1..N as column groups.
Each group has (name, organisation type, address, region, country, website).
Parser unpivots to long format: one row per (project, organisation, role).
"""

import os, sys, io, csv, json, hashlib, re, uuid
from datetime import date, datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_lib

GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "erasmus")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")
ENHETER_BUCKET = os.environ.get("ENHETER_BUCKET", "sondre_brreg_data")
ENHETER_PREFIX = os.environ.get("ENHETER_PREFIX", "enheter/parsed/v1/state")

TRACKED_FIELDS_ERASMUS = ["org_name", "org_role", "action_type", "grant_eur"]

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()), ("document_id", pa.string()), ("data_source", pa.string()),
    ("event_type", pa.string()), ("event_subtype", pa.string()), ("summary", pa.string()),
    ("changed_fields", pa.string()), ("valid_time", pa.string()), ("detected_time", pa.string()),
    ("details_json", pa.string()), ("source_run_mode", pa.string()), ("run_id", pa.string()),
])

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()), ("first_seen", pa.string()), ("last_seen", pa.string()),
    ("n_participations", pa.int32()), ("actions", pa.string()),
])


def load_enheter_lookup(bucket_name, prefix):
    client = gcs_lib.Client()
    bucket = client.bucket(bucket_name)
    dates = []
    iterator = bucket.list_blobs(prefix=f"{prefix}/", delimiter="/")
    for page in iterator.pages:
        for blob in page:
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.append(name.replace(".parquet", ""))
    dates.sort()
    if not dates:
        return {}
    latest = dates[-1]
    print(f"  Loading enheter: {prefix}/{latest}.parquet", flush=True)
    blob = bucket.blob(f"{prefix}/{latest}.parquet")
    table = pq.read_table(io.BytesIO(blob.download_as_bytes()), columns=["org_nr", "name"])
    lookup = {}
    for orgnr, name in zip(table.column("org_nr").to_pylist(), table.column("name").to_pylist()):
        if orgnr and name:
            lookup[name.upper().strip()] = str(orgnr).strip()
    print(f"  Enheter lookup: {len(lookup):,} entries", flush=True)
    return lookup


def extract_partners_from_row(row, headers):
    """Extract coordinator + all partners from a wide-format CSV row.

    Returns list of dicts with keys: name, country, org_type, role, address, region.
    """
    partners = []

    coord_name = row.get("Coordinating organisation name", "").strip()
    coord_country = row.get("Coordinator's country", "").strip()
    if coord_name:
        partners.append({
            "name": coord_name,
            "country": coord_country,
            "org_type": row.get("Coordinating organisation type", ""),
            "role": "coordinator",
            "address": row.get("Coordinator's address", ""),
            "region": row.get("Coordinator's region", ""),
        })

    for i in range(1, 50):
        name_key = f"Partner {i} name"
        if name_key not in row:
            break
        pname = (row.get(name_key) or "").strip()
        if not pname:
            continue
        partners.append({
            "name": pname,
            "country": (row.get(f"Partner {i} country") or "").strip(),
            "org_type": row.get(f"Partner {i} organisation type", ""),
            "role": "partner",
            "address": row.get(f"Partner {i} address", ""),
            "region": row.get(f"Partner {i} region", ""),
        })

    return partners


def parse_csv_file(csv_bytes, filename, enheter_lookup):
    """Parse one Erasmus+ CSV. Returns (resolved, unresolved) lists."""
    text = csv_bytes.decode("utf-8", errors="replace")

    delimiter = ";"
    if text[:500].count(",") > text[:500].count(";"):
        delimiter = ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    headers = reader.fieldnames or []

    resolved = []
    unresolved = []

    for row in reader:
        project_id = row.get("Project Identifier", "").strip()
        project_title = row.get("Project Title", "").strip()
        action_type = row.get("Action Type", "").strip()
        call_year = row.get("Call year", "").strip()
        grant_eur = row.get("EU Grant award in euros (This amount represents the grant awarded after the selection stage and is indicative. Please note that any changes made during or after the project's lifetime will not be reflected here.)", "").strip()
        if not grant_eur:
            for k, v in row.items():
                if k and "grant" in k.lower() and "euro" in k.lower():
                    grant_eur = (v or "").strip()
                    break

        partners = extract_partners_from_row(row, headers)

        for p in partners:
            if p["country"] != "NO":
                continue

            name_upper = p["name"].upper().strip()
            orgnr = enheter_lookup.get(name_upper)
            method = "name_exact" if orgnr else None

            h = hashlib.sha256(f"{project_id}|{p['name']}|{p['role']}".encode()).hexdigest()[:16]

            entry = {
                "orgnr": orgnr,
                "orgnr_resolution_method": method,
                "project_id": project_id,
                "project_title": project_title,
                "action_type": action_type,
                "call_year": call_year,
                "grant_eur": grant_eur,
                "org_name": p["name"],
                "org_type": p["org_type"],
                "org_role": p["role"],
                "org_country": p["country"],
                "org_region": p["region"],
                "source_file": filename,
                "content_hash": h,
            }

            if orgnr:
                resolved.append(entry)
            else:
                unresolved.append(entry)

    return resolved, unresolved


SNAPSHOT_SCHEMA = pa.schema([
    ("project_id", pa.string()), ("orgnr", pa.string()),
    ("content_hash", pa.string()),
] + [(f, pa.string()) for f in TRACKED_FIELDS_ERASMUS])


def run_cdc(resolved_rows, run_date, run_mode, bucket_name, prefix):
    run_id = str(uuid.uuid4())[:8]
    detected_time = datetime.now(timezone.utc).isoformat()
    client = gcs_lib.Client()
    bucket = client.bucket(bucket_name)

    def read_pq(path):
        blob = bucket.blob(path)
        if not blob.exists():
            return None
        return pq.read_table(io.BytesIO(blob.download_as_bytes()))

    def write_pq(table, path):
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        buf.seek(0)
        bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")

    parsed_blobs = [b for b in bucket.list_blobs(prefix=f"{prefix}/parsed/") if b.name.endswith(".parquet")]
    parsed_dates = sorted(set(b.name.split("/")[-1].replace(".parquet","") for b in parsed_blobs))
    prev_dates = [d for d in parsed_dates if d < run_date]
    old_snaps = {}
    if prev_dates:
        prev_t = read_pq(f"{prefix}/parsed/{prev_dates[-1]}.parquet")
        if prev_t:
            d = prev_t.to_pydict()
            old_snaps = {}
            for i in range(prev_t.num_rows):
                key = (d["project_id"][i], d["orgnr"][i])
                old_snaps[key] = {"content_hash": d["content_hash"][i]}
                for f in TRACKED_FIELDS_ERASMUS:
                    if f in d:
                        old_snaps[key][f] = d[f][i]

    pool_t = read_pq(f"{prefix}/cdc/pool.parquet")
    pool = {}
    if pool_t:
        d = pool_t.to_pydict()
        pool = {d["orgnr"][i]: {"first_seen": d["first_seen"][i], "last_seen": d["last_seen"][i],
                                "n_participations": d["n_participations"][i], "actions": d["actions"][i]} for i in range(pool_t.num_rows)}

    changelog_rows = []
    new_snaps = {}
    new_count = 0
    mod_count = 0

    for row in resolved_rows:
        key = (row["project_id"], row["orgnr"])
        h = row["content_hash"]
        old_h = old_snaps.get(key)
        doc_id = f"erasmus-{hashlib.sha256(f'{row["project_id"]}|{row["orgnr"]}'.encode()).hexdigest()[:12]}"

        new_snaps[key] = {"project_id": row["project_id"], "orgnr": row["orgnr"],
                          "org_name": row["org_name"], "org_role": row["org_role"], "content_hash": h}

        if run_mode == "bootstrap" or old_h is None:
            event_type = "new"
            changed_fields = None
            new_count += 1
        elif old_h != h:
            event_type = "modified"
            changed_fields = json.dumps(["content_hash"])
            mod_count += 1
        else:
            continue

        action = row.get("action_type", "")
        summary = " — ".join(filter(None, [
            row["org_role"], row["org_name"][:40],
            f"{action} {row.get('call_year','')}",
            row.get("project_title", "")[:30],
        ]))

        details = {k: v for k, v in row.items() if k != "content_hash"}
        changelog_rows.append({
            "orgnr": row["orgnr"],
            "document_id": doc_id,
            "data_source": "erasmus",
            "event_type": event_type,
            "event_subtype": f"erasmus_{row['org_role']}",
            "summary": summary,
            "changed_fields": changed_fields,
            "valid_time": f"{row.get('call_year','2024')}-01-01" if row.get("call_year") else run_date,
            "detected_time": detected_time,
            "details_json": json.dumps(details, ensure_ascii=False),
            "source_run_mode": run_mode,
            "run_id": run_id,
        })

        orgnr = row["orgnr"]
        if orgnr in pool:
            pool[orgnr]["last_seen"] = run_date
            pool[orgnr]["n_participations"] += 1
            existing = set(pool[orgnr]["actions"].split(",")) if pool[orgnr]["actions"] else set()
            existing.add(action)
            pool[orgnr]["actions"] = ",".join(sorted(existing))
        else:
            pool[orgnr] = {"first_seen": run_date, "last_seen": run_date, "n_participations": 1, "actions": action}

    if run_mode != "bootstrap":
        for key, old_h in old_snaps.items():
            if key not in new_snaps:
                changelog_rows.append({
                    "orgnr": key[1], "document_id": f"erasmus-{hashlib.sha256(f'{key[0]}|{key[1]}'.encode()).hexdigest()[:12]}",
                    "data_source": "erasmus", "event_type": "disappeared", "event_subtype": "erasmus_participation_ended",
                    "summary": f"Participation ended: {key[0]}", "changed_fields": None,
                    "valid_time": run_date, "detected_time": detected_time, "details_json": None,
                    "source_run_mode": run_mode, "run_id": run_id,
                })

    if changelog_rows:
        write_pq(pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA),
                 f"{prefix}/cdc/changelog/{run_date}.parquet")

    snap_rows = list(new_snaps.values())
    if snap_rows:
        write_pq(pa.Table.from_pylist(snap_rows, schema=SNAPSHOT_SCHEMA),
                 f"{prefix}/parsed/{run_date}.parquet")

    if pool:
        write_pq(pa.Table.from_pylist([{"orgnr": k, **v} for k, v in pool.items()], schema=POOL_SCHEMA),
                 f"{prefix}/cdc/pool.parquet")

    return {"new": new_count, "modified": mod_count, "changelog_rows": len(changelog_rows), "pool_size": len(pool)}


def main():
    snapshot = SNAPSHOT_DATE if SNAPSHOT_DATE else None

    print(f"{'='*60}\n  erasmus-parser — mode: {RUN_MODE}\n  {date.today().isoformat()}\n  GCS: gs://{GCS_BUCKET}/{GCS_PREFIX}/\n{'='*60}", flush=True)

    client = gcs_lib.Client()
    bucket = client.bucket(GCS_BUCKET)

    prefix = f"{GCS_PREFIX}/raw/"
    dates = set()
    iterator = bucket.list_blobs(prefix=prefix, delimiter="/")
    for page in iterator.pages:
        for p in page.prefixes:
            d = p.rstrip("/").split("/")[-1]
            if len(d) == 10:
                dates.add(d)
    dates = sorted(dates)
    if not dates:
        print("  No snapshots.", flush=True)
        sys.exit(1)

    snapshot = snapshot or dates[-1]
    print(f"  Using snapshot: {snapshot}", flush=True)

    enheter_lookup = load_enheter_lookup(ENHETER_BUCKET, ENHETER_PREFIX)

    csv_blobs = [b for b in bucket.list_blobs(prefix=f"{GCS_PREFIX}/raw/{snapshot}/downloads/") if b.name.endswith(".csv")]
    print(f"  CSV files: {len(csv_blobs)}", flush=True)

    all_resolved = []
    all_unresolved = []

    for blob in csv_blobs:
        filename = blob.name.split("/")[-1]
        csv_bytes = blob.download_as_bytes()
        resolved, unresolved = parse_csv_file(csv_bytes, filename, enheter_lookup)
        print(f"  {filename[:60]}: {len(resolved)} resolved, {len(unresolved)} unresolved NO", flush=True)
        all_resolved.extend(resolved)
        all_unresolved.extend(unresolved)

    print(f"\n  Total: {len(all_resolved):,} resolved, {len(all_unresolved):,} unresolved", flush=True)

    run_mode = "bootstrap" if RUN_MODE == "bootstrap" else "daily"
    stats = run_cdc(all_resolved, date.today().isoformat(), run_mode, GCS_BUCKET, GCS_PREFIX)
    print(f"  CDC: {stats}", flush=True)

    if all_unresolved:
        ur_schema = pa.schema([("project_id", pa.string()), ("org_name", pa.string()),
                               ("org_role", pa.string()), ("action_type", pa.string()),
                               ("call_year", pa.string()), ("source_file", pa.string())])
        ur = [{"project_id": r["project_id"], "org_name": r["org_name"], "org_role": r["org_role"],
               "action_type": r.get("action_type",""), "call_year": r.get("call_year",""),
               "source_file": r.get("source_file","")} for r in all_unresolved]
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(ur, schema=ur_schema), buf, compression="zstd")
        buf.seek(0)
        bucket.blob(f"{GCS_PREFIX}/unresolved/{date.today().isoformat()}.parquet").upload_from_file(
            buf, content_type="application/octet-stream")
        print(f"  Unresolved written: {len(all_unresolved)} rows", flush=True)


if __name__ == "__main__":
    main()
