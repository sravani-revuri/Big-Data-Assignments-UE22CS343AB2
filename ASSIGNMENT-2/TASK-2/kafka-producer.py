#!/usr/bin/env python3
import sys
from kafka import KafkaProducer

topic_problem = sys.argv[1]
topic_competition = sys.argv[2]
topic_solution = sys.argv[3]


producer = KafkaProducer(value_serializer=lambda v: v.encode('utf-8'))

for line in sys.stdin:
    
    if line == "EOF":
        producer.send(topic_competition, value='EOF')
        producer.send(topic_problem, value='EOF')
        producer.send(topic_solution, value='EOF')
        break
    line = line.strip()

    if line.startswith("problem"):
        producer.send(topic_problem, value=line)
        producer.send(topic_solution, value=line)
    elif line.startswith("competition"):
        producer.send(topic_competition, value=line)
        producer.send(topic_solution, value=line)
    else:
        producer.send(topic_solution, value=line)

producer.flush()
producer.close()
