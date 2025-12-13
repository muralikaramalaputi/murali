# background_stage1.py
"""
Stage 1: Background ingestion of SAP / Vault / PowerBI / PO / Invoice
→ Cleansing → Merging by part_number → Upsert into ONE flat table part_master.
"""

import os
import sys
from collections import defaultdict

import pandas as pd

from config import SOURCES_DIRS, OUTPUT_DIR
from ingestion_utils import load_file
from cleansing import cleanup_pipeline
from enrichment_text import enrich_from_description
from merge_logic import merge_records_by_part_number
from db import init_db, upsert_part_master

# Force unbuffered output for Render logs
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)


def load_all_sources() -> pd.DataFrame:
    all_rows = []
    for system, folder in SOURCES_DIRS.items():
        if not os.path.isdir(folder):
            print(f"⚠️  Folder not found: {folder}", flush=True)
            continue
        for fname in os.listdir(folder):
            path = os.path.join(folder, fname)
            print(f"📄 [{system}] Processing: {path}", flush=True)
            df = load_file(path)
            if df is None or df.empty:
                continue
            df["source_system"] = system
            df["source_file"] = fname
            all_rows.append(df)
    if not all_rows:
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


def clean_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = cleanup_pipeline(df)
    df = enrich_from_description(df)
    return df


def run_stage1():
    print("=" * 80, flush=True)
    print("🚀 Stage 1: Background Ingestion Started", flush=True)
    print("=" * 80, flush=True)

    try:
        init_db()
        print("✅ Database initialized", flush=True)

        df_raw = load_all_sources()
        print(f"📊 Raw rows loaded: {len(df_raw)}", flush=True)

        if df_raw.empty:
            print("⚠️  No data loaded from sources - folders may be empty", flush=True)
            print("✅ Stage 1 complete (no data to process)", flush=True)
            return

        df_clean = clean_pipeline(df_raw)
        print(f"✅ Cleaned {len(df_clean)} rows", flush=True)

        # Convert to records
        records = df_clean.to_dict(orient="records")

        # Group by part number
        grouped = {}
        for r in records:
            pn = r.get("part_number")
            if not pn:
                continue
            grouped.setdefault(pn, []).append(r)

        print(f"📊 Grouped into {len(grouped)} unique part numbers", flush=True)

        # Merge rows for each part
        merged_records = []
        for pn, rows in grouped.items():
            merged = merge_records_by_part_number(rows)
            merged_records.append(merged)

        print(f"📊 Unique merged part_numbers: {len(merged_records)}", flush=True)

        # Upsert
        if merged_records:
            upsert_part_master(merged_records)
            print(f"✅ Upserted {len(merged_records)} records to database", flush=True)
        else:
            print("⚠️  No records to upsert", flush=True)

        # Save snapshot for inspection
        snapshot_path = os.path.join(OUTPUT_DIR, "stage1_master_snapshot.xlsx")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        pd.DataFrame(merged_records).to_excel(snapshot_path, index=False)
        print(f"📂 Snapshot saved to: {snapshot_path}", flush=True)
        print("=" * 80, flush=True)
        print("✅ Stage 1 complete successfully!", flush=True)
        print("=" * 80, flush=True)

    except Exception as e:
        print("=" * 80, flush=True)
        print(f"❌ Stage 1 failed: {e}", flush=True)
        print("=" * 80, flush=True)
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_stage1()