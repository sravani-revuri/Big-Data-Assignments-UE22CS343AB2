#!/usr/bin/env python3
import sys

city_counts = {}

for line in sys.stdin:
    line=line.strip()
    if line:
            city,status=line.split('\t')
            status=int(status)
            if(city not in city_counts):
                city_counts[city]={"profit_stores": 0, "loss_stores": 0}
            if(status==1):
                city_counts[city]["profit_stores"] += 1
            elif(status==0):
                city_counts[city]["loss_stores"] += 1

for city,counts in city_counts.items():
    print(f'{{"city": "{city}", "profit_stores": {counts["profit_stores"]}, "loss_stores": {counts["loss_stores"]}}}')
