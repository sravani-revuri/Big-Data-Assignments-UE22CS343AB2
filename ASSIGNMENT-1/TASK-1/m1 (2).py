#!/usr/bin/env python3

import sys
import json

def calc(j):
    city = j.get("city", "Unknown")  # Default to 'Unknown' if city is not provided
    cat = j.get("categories", [])  # Default to an empty list if no categories are provided
    s_data = j.get("sales_data", {})  # Default to an empty dict if no sales data is provided

    # Check if sales data is present
    if not s_data:
        print(f"{city}\tNo sales data")
        return

    prof = 0
    has_data = False

    for k in cat:
        if k in s_data:
            category_data = s_data[k]
            revenue = category_data.get("revenue")
            cogs = category_data.get("cogs")

            # Check if revenue and cogs are both present
            if revenue is not None and cogs is not None:
                prof += (revenue - cogs)
                has_data = True
            else:
                continue
        else:
            continue

    if has_data:
        if prof > 0:
            print(f"{city}\t1")
        else:
            print(f"{city}\t0")
    else:
        print(f"{city}\t0")

for line in sys.stdin:
    line = line.strip()
    if line in ["[", "]"]:
        continue
    if line.endswith(","):
        line = line[:-1]
        j = json.loads(line)
        calc(j)