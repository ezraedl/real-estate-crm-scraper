"""
Test Rentcast scraping on several properties.

Fetches properties from the DB (by address or by ID) and runs
RentcastService.fetch_and_save_rent_estimate for each. Prints results.

With --address: fetches rent for that address only (no DB). No MONGODB_URI needed.

Usage:
  python scripts/test_rentcast.py --address "851 W 25th St, Indianapolis, IN 46208"
  python scripts/test_rentcast.py                    # test up to 3 properties from DB
  python scripts/test_rentcast.py --limit 5          # test up to 5
  python scripts/test_rentcast.py --ids "id1,id2"    # test specific property_ids

Requires: .env with MONGODB_URI (except for --address); playwright install chromium.
"""

import argparse
import asyncio
import os
import sys
from urllib.parse import urlparse

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


async def main():
    parser = argparse.ArgumentParser(description="Test Rentcast scraping on several properties")
    parser.add_argument(
        "--address",
        type=str,
        default=None,
        help='Test a single address (e.g. "851 W 25th St, Indianapolis, IN 46208"). No DB.',
    )
    parser.add_argument("--limit", type=int, default=3, help="Max number of properties (default 3)")
    parser.add_argument(
        "--ids",
        type=str,
        default=None,
        help="Comma-separated property_ids to test (overrides --limit and address filter)",
    )
    args = parser.parse_args()

    from services.rentcast_service import RentcastService

    # --address: fetch only, no DB
    if args.address:
        addr = args.address.strip()
        if not addr:
            print("--address must be non-empty.")
            return
        print(f"Testing Rentcast for: {addr}\n")
        svc = RentcastService(db=None)
        property_dict = {"address": {"formatted_address": addr}, "description": {}}
        data = await svc.fetch_rent_estimate(property_dict)
        if data:
            comps = data.get("comparables") or []
            comp_n = f"  comps={len(comps)}" if comps else ""
            print(f"  OK   rent=${data.get('rent')}  low=${data.get('rent_range_low')}  high=${data.get('rent_range_high')}{comp_n}")
        else:
            print("  No rent found (parse error or address not in Rentcast).")
        print("Done.")
        return

    from config import settings
    from motor.motor_asyncio import AsyncIOMotorClient

    # Minimal DB connection (avoids full db.connect + enrichment pipeline)
    parsed = urlparse(settings.MONGODB_URI)
    db_name = parsed.path.lstrip("/").split("?")[0] or "mls_scraper"
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client[db_name]
    # Wrapper so RentcastService gets .properties_collection
    class _Db:
        def __init__(self, coll):
            self.properties_collection = coll

    db_wrapper = _Db(db.properties)

    if args.ids:
        ids = [s.strip() for s in args.ids.split(",") if s.strip()]
        cursor = db_wrapper.properties_collection.find({"property_id": {"$in": ids}})
        properties = await cursor.to_list(length=len(ids))
        if len(properties) < len(ids):
            found = {p["property_id"] for p in properties}
            print(f"Warning: only found {len(properties)} of {len(ids)} requested: {found}")
    else:
        cursor = db_wrapper.properties_collection.find(
            {
                "address": {"$exists": True},
                "$or": [
                    {"address.formatted_address": {"$exists": True, "$ne": ""}},
                    {"address.city": {"$exists": True, "$ne": ""}},
                ],
            }
        ).limit(args.limit)
        properties = await cursor.to_list(length=args.limit)

    if not properties:
        print("No properties found. Add --ids=id1,id2 or ensure DB has properties with address.")
        return

    print(f"Testing Rentcast on {len(properties)} property(ies)\n")
    svc = RentcastService(db_wrapper)

    for i, p in enumerate(properties, 1):
        pid = p.get("property_id", "?")
        addr = (p.get("address") or {}).get("formatted_address") or (
            (p.get("address") or {}).get("street") or ""
        )
        print(f"[{i}/{len(properties)}] {pid}  {addr[:60]}{'...' if len(str(addr)) > 60 else ''}")
        ok = await svc.fetch_and_save_rent_estimate(pid, p)
        if ok:
            updated = await db_wrapper.properties_collection.find_one(
                {"property_id": pid}, {"rent_estimation": 1}
            )
            re = (updated or {}).get("rent_estimation") or {}
            print(f"  OK   rent=${re.get('rent')}  low=${re.get('rent_range_low')}  high=${re.get('rent_range_high')}")
        else:
            print("  FAIL (no address, parse error, or no rent found)")
        print()

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
