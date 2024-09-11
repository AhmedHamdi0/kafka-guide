# To start broker server using KRaft
kafka-storage.sh random-uuid
kafka-storage.sh format -t nU_cvGYhSimJjUbSJL2W1w -c ~/Programs/Kafka/kafka_2.13-3.8.0/config/kraft/server.properties
kafka-server-start.sh ~/Programs/Kafka/kafka_2.13-3.8.0/config/kraft/server.properties

# To create a topic
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic [topic_name] --create --partitions 3 --replication-factor 1

# To start consuming the a topic
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic [topic_name]
