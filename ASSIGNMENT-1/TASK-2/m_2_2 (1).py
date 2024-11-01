#!/usr/bin/env python3

#htoughts
#MAPPER-REDUCER 2:IF cX and ts matches but endpoint doesnt take the one in chronological order form sorted inp.
#if serV_down==3 then append 500new to answer input if not then append 200new to answer.
#success ratio calculation???
#key returned to reducer will be cx and timestamp value will be remaining values

#sort based on timestamp and request id, works even when sorted with either one

import sys
for line in sys.stdin:
    line=line.strip()
    rX,cX,ts,endpoint,serv_down,stat_code=line.split('\t')
    key=f"{ts} {rX}"
    value=f"{cX} {endpoint} {serv_down} {stat_code}"
    print(f"{key}\t{value}")
    
