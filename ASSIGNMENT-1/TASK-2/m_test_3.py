#!/usr/bin/env python3

#simply pass the values for price calculation 

import sys
for line in sys.stdin:
    key,val=line.strip().split('\t')
    print(f"{key}\t{val}")
