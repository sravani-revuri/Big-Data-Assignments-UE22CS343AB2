#!/usr/bin/env python3

# MAPPER-REDUCER 3: Handle access to the same endpoint by multiple clients
# Clients are served based on available servers (3 - serv_down), with 200 for those who get served, and 500 otherwise.
#dictionary which keeps track of timestamp and endpoint and holds the value of servers down
#decremented each successful request

import sys

time_dict = {}

for line in sys.stdin:
    key, value = line.strip().split('\t')
    ts, rX = key.split()
    cX, endpoint, serv_down, stat_code = value.split()
    if serv_down == '3.0':
        val = f"{rX} {ts} {endpoint} {serv_down} {stat_code} 500"
        print(f"{cX}\t{val}")
        continue
    available_servers = 3 - int(float(serv_down))
    if ts not in time_dict:
        time_dict[ts] = {}
    if endpoint not in time_dict[ts]:
        time_dict[ts][endpoint] = available_servers
    if time_dict[ts][endpoint] > 0:
        val = f"{rX} {ts} {endpoint} {serv_down} {stat_code} 200"
        time_dict[ts][endpoint] -= 1
    else:
        val = f"{rX} {ts} {endpoint} {serv_down} {stat_code} 500"
    print(f"{cX}\t{val}")