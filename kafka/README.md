\# Kafka Setup



\## Start Kafka

docker compose up -d



\## Check it is running

docker ps



\## Create topics

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic covid-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic covid-cleaned --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1



\## List topics

docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092



\## Describe a topic

docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic covid-data --bootstrap-server localhost:9092



\## Send test messages (Producer)

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic covid-data --bootstrap-server localhost:9092



\## Read messages (Consumer)

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic covid-data --bootstrap-server localhost:9092 --from-beginning



\## Stop Kafka

docker compose down

## Verify Python connectivity

Tested with the Member 1 producer (sent 390 records, received in the console consumer):

python producer/producer.py data/covid\_19\_data.csv 2021-05-28 2021-05-29



