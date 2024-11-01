#!/usr/bin/env python3



#MAPPER-REDUCER 1: APPENDING OF STATUS CODE TO 
import sys

def req(data):
    rX = data[0]
    cX = data[1]
    endpoint = data[2]
    ts = data[3]
    # Handle missing serv_down field
    if len(data)== 5:
        serv_down = data[4] 
    else:
        serv_down='0.0'
    key = f"{rX} R"
    val = f"{cX} {endpoint} {ts} {serv_down}"
    return f"{key}\t{val}"

def stats(data):
    rXs = data[0]
    stat_code = data[1]
    key = f"{rXs} S"
    return f"{key}\t{stat_code}"

for line in sys.stdin:
    fields = line.strip().split()
    if len(fields) == 5 or len(fields) == 4:  # Handle missing serv_down
        print(req(fields))
    elif len(fields) == 2:
        print(stats(fields))