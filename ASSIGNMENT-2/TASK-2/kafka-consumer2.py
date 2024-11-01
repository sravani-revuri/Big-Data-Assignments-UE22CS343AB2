#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys

topic_competition=sys.argv[2]
consumer=KafkaConsumer(topic_competition, value_deserializer=lambda m: m.decode('utf-8'))
def calc(components):
    status=components[7]
    dif_Sc=components[5]
    run__time=float(components[-2])
    time_taken=float(components[-1])
    Runtime_bonus = 10000/run__time
    Time_taken_penalty = 0.25*time_taken
    Bonus = max(1,(1 + Runtime_bonus - Time_taken_penalty))
    if(status=="Passed"):
        Status_Score=100
    elif(status=="TLE"):
        Status_Score=20
    elif(status=="Failed"):
        Status_Score=0
    if(dif_Sc=="Hard"):
        Difficulty_Score=3
    elif(dif_Sc=="Medium"):
        Difficulty_Score=2
    elif(dif_Sc=="Easy"):
        Difficulty_Score=1
    Submission_Points = Status_Score * Difficulty_Score * Bonus
    return int(Submission_Points)
competition_dict={}
for message in consumer:
    message_value = message.value.strip()
    if message_value == "EOF":
        break
    components = message_value.split(' ')
    if components[0]!="competition":
        continue
    competitio=components[1]
    user=components[2]
    if competitio not in competition_dict:
        competition_dict[competitio]={}
    val=calc(components)
    if user not in competition_dict[competitio]:
        competition_dict[competitio][user]=0
    competition_dict[competitio][user]+=val
output = {}
for competitio in sorted(competition_dict.keys()):
    output[competitio] = {user: points for user, points in sorted(competition_dict[competitio].items())}
print(json.dumps(output, indent=4, sort_keys=True))

    

    
    