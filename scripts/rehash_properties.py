import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from bson import ObjectId
except Exception:
    ObjectId = None

from pymongo import MongoClient, UpdateOne

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from models import Property


def load_mongo_uri() -> Optional[str]:
    uri = os.getenv("MONGODB_URI")
    if uri:
        return uri
    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(env_path):
        return None
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("MONGODB_URI="):
                return line.split("=", 1)[1].strip().strip('"')
    return None


def build_query(args: argparse.Namespace) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    if args.status:
        query["status"] = args.status
    if args.mls_status:
        query["mls_status"] = args.mls_status
    if args.zip_code:
        query["address.zip_code"] = args.zip_code
    return query


def main() -> int:
    parser = argparse.ArgumentParser(description="Recalculate content_hash for properties.")
    parser.add_argument("--status", help="Filter by status (e.g., OFF_MARKET)")
    parser.add_argument("--mls-status", dest="mls_status", help="Filter by mls_status (e.g., DELISTED)")
    parser.add_argument("--zip", dest="zip_code", help="Filter by address.zip_code")
    parser.add_argument("--limit", type=int, default=0, help="Max number of properties to process")
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write updates")
    parser.add_argument("--batch", type=int, default=500, help="Bulk write batch size")
    args = parser.parse_args()

    uri = load_mongo_uri()
    if not uri:
        print("MONGODB_URI not found in environment or .env")
        return 1

    client = MongoClient(uri)
    db = client.get_database("mls_scraper")
    col = db.get_collection("properties")

    query = build_query(args)
    cursor = col.find(query)
    if args.limit and args.limit > 0:
        cursor = cursor.limit(args.limit)

    total = 0
    changed = 0
    skipped = 0
    errors = 0
    bulk_ops = []

    for doc in cursor:
        total += 1
        try:
            model_fields = set(Property.model_fields.keys())
            payload = {k: doc.get(k) for k in model_fields if k in doc}

            # Normalize ObjectId contact refs to strings for Pydantic validation
            if ObjectId is not None:
                for key in ("agent_id", "broker_id", "builder_id", "office_id"):
                    val = payload.get(key)
                    if isinstance(val, ObjectId):
                        payload[key] = str(val)

            prop = Property(**payload)
            new_hash = prop.generate_content_hash()
            existing_hash = doc.get("content_hash")

            if existing_hash == new_hash:
                skipped += 1
                continue

            update = {
                "content_hash": new_hash,
                "last_content_updated": datetime.now(timezone.utc),
                "last_updated": datetime.now(timezone.utc),
            }

            changed += 1
            if not args.dry_run:
                bulk_ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update}))
                if len(bulk_ops) >= args.batch:
                    col.bulk_write(bulk_ops, ordered=False)
                    bulk_ops = []
        except Exception as e:
            errors += 1
            print(f"[ERROR] property_id={doc.get('property_id')} err={e}")

    if bulk_ops and not args.dry_run:
        col.bulk_write(bulk_ops, ordered=False)

    print(
        f"Processed={total} Changed={changed} Skipped={skipped} "
        f"Errors={errors} DryRun={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
