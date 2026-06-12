#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

bucket = os.getenv("PMTILES_BUCKET", "pmtiles")
prefix = "argeles"  # dossier dans le bucket

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SERVICE_KEY"])

items = sb.storage.from_(bucket).list(prefix)

for item in sorted(items, key=lambda x: x.get("name", "")):
    name = item["name"]
    size = item.get("metadata", {}).get("size") or item.get("size")
    print(f"{prefix}/{name}  ({size} bytes)" if size else f"{prefix}/{name}")