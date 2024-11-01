#!/usr/bin/env python3

#dictinoary to keep track of prices per customer, if new customer comes in create an entry , if actual code is 200 then increment price for that endpoint
#if predicted code != actual code increment false predictions , i.e '0' else increment '1' if equal
#print customer ratio of '1'/('1'+'0') and cost at the end
cust_count={}
prices={'user/profile': 100,
        'user/settings': 200,
        'order/history': 300,
        'order/checkout': 400,
        'product/details': 500,
        'product/search': 600,
        'cart/add': 700,
        'cart/remove': 800,
        'payment/submit': 900,
        'support/ticket': 1000}

import sys
for line in sys.stdin:
    key,val=line.strip().split('\t')
    #line.split('\t')
    cX=key
    rX,ts,endpoint,serv_down,stat_code,act_code=val.split()
    if cX not in cust_count:
        cust_count[cX] = {"1": 0, "0": 0, "curr_rat": 0.0, "total_cost": 0}
    if act_code=='200':
        cust_count[cX]["total_cost"]+=prices[endpoint]
    if(act_code==stat_code):
        cust_count[cX]["1"]+=1
        #cust_count[cX]["curr_rat"]=(cust_count[cX]["1"])/((cust_count[cX]["1"]+cust_count[cX]["0"]))
    else:
        cust_count[cX]["0"]+=1
    #cust_count[cX]["curr_rat"]=(cust_count[cX]["1"])/((cust_count[cX]["1"]+cust_count[cX]["0"]))

for cX, data in cust_count.items():
    denom=data['1']+data['0']
    print(f"{cX} {data['1']}/{denom} {data['total_cost']}")
