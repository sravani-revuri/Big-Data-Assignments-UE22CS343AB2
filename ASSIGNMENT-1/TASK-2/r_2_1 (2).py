#!/usr/bin/env python3

#removal of same client accessing different endpoints at same time, first entry is only taken according to sorting by request id
import sys
arRay =[]
keep_track={}
client_val={}
for line in sys.stdin:
    key,val = line.strip().split('\t')
    if(key[-1]=='R'):
        cX,endpoint,ts,serv_down=val.split(' ')
        if((cX,ts) not in client_val):
            keep_track[key[0:-2]]=1
            arRay=[key[0:-2],cX,ts,endpoint,serv_down]
            client_val[(cX,ts)]=key[0:-2]
        else:
            continue
    elif(key[-1]=='S'):
        if key[0:-2] in keep_track:
            if(keep_track[key[0:-2]]==1) and client_val[(cX,ts)]==key[0:-2]:
                print(f"{key[0:-2]}\t{arRay[1]}\t{arRay[2]}\t{arRay[3]}\t{arRay[4]}\t{val}")
                keep_track[key[0:-2]]-=1
            else:
                continue