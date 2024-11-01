#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys

topic_problem=sys.argv[1]
consumer=KafkaConsumer(topic_problem, value_deserializer=lambda m: m.decode('utf-8'))

language_dict = {}
category_dict = {}

for message in consumer:
    message_value = message.value.strip()
    if message_value == "EOF":
        break
    components = message_value.split(' ')
    if components[0]!="problem":
        continue
    category=components[3]
    status=components[6]
    language=components[7]
    if language not in language_dict:
        language_dict[language]=0 
    if category not in category_dict:
        category_dict[category] = {"passed": 0, "total": 0}
    if status == 'Passed':
        category_dict[category]["passed"] += 1
        category_dict[category]["total"] += 1
    else:
        category_dict[category]["total"] += 1
    language_dict[language]+=1

max_count = 0
most_used_language = []

for language, count in language_dict.items():
    if count > max_count:
        max_count = count
        most_used_language = [language]
    elif count == max_count:
        most_used_language.append(language)
most_used_language.sort()

min_pass_ratio = float('inf')
dif= []
for category, val in category_dict.items():
    total=val['total']
    passed=val['passed']
    if total>0:
        pass_ratio=passed/total
        if pass_ratio<min_pass_ratio:
            min_pass_ratio=pass_ratio
            dif=[category] 
        elif pass_ratio==min_pass_ratio:
            dif.append(category)
dif.sort()
final = {
    "most_used_language": most_used_language,
    "most_difficult_category": dif
}
print(json.dumps(final, indent=4))
