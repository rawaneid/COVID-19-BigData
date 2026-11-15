from kafka import KafkaProducer
import json
import time

# Connect Python to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Sample COVID-19 data
covid_data = {
    "country": "Egypt",
    "cases": 1200,
    "deaths": 50
}

# Send data to Kafka topic
producer.send('covid-data', value=covid_data)

producer.flush()
producer.close()

print("COVID-19 data sent successfully!")