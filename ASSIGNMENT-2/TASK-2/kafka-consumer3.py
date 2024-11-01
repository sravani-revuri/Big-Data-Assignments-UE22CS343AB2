#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys

topic_solution = sys.argv[3]
consumer = KafkaConsumer(topic_solution, value_deserializer=lambda m: m.decode('utf-8'))

consumer_dict = {}
upvote_count = {}

def calculate_submission_points(user, status, dif_Sc, runtime):
    K = 32
    if status == "Passed":
        Status_Score = 1
    elif status == "TLE":
        Status_Score = 0.2
    elif status == "Failed":
        Status_Score = -0.3
    if dif_Sc == "Hard":
        Difficulty_Score = 1
    elif dif_Sc == "Medium":
        Difficulty_Score = 0.7
    elif dif_Sc == "Easy":
        Difficulty_Score = 0.3
    Runtime_bonus = 10000 / runtime if runtime > 0 else 0
    Submission_Points = K * (Status_Score * Difficulty_Score) + Runtime_bonus
    return Submission_Points

def prob(components):
    user = components[1]
    status = components[-3]
    dif_Sc = components[-5]
    runtime = float(components[-1])

    if user not in consumer_dict:
        consumer_dict[user] = 1200

    Submission_Points = calculate_submission_points(user, status, dif_Sc, runtime)
    consumer_dict[user] += Submission_Points

def conm(components):
    user = components[2]
    status = components[-4]
    dif_Sc = components[-6]
    runtime = float(components[-2])

    if user not in consumer_dict:
        consumer_dict[user] = 1200

    Submission_Points = calculate_submission_points(user, status, dif_Sc, runtime)
    consumer_dict[user] += Submission_Points

for message in consumer:
    message_value = message.value.strip()
    if message_value == "EOF":
        break
    components = message_value.split(' ')
    if components[0] == "problem":
        prob(components)
    elif components[0] == "competition":
        conm(components)
    elif components[0] == "solution":
        user = components[1]
        up = int(components[-1])
        if user not in upvote_count:
            upvote_count[user] = 0
        upvote_count[user] += up
    else:
        continue

max_count = 0
max_upvote_list = []
for user, count in upvote_count.items():
    if count > max_count:
        max_count = count
        max_upvote_list = [user]
    elif count == max_count:
        max_upvote_list.append(user)

max_upvote_list.sort()

output = {
    "best_contributor": max_upvote_list,
    "user_elo_rating": {user: int(score) for user, score in sorted(consumer_dict.items(), key=lambda item: item[0])}  # Sorted by user_id lexicographically
}

print(json.dumps(output, indent=4))
