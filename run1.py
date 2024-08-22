from data import push_data
import threading

topic = "topic1"
data_push = push_data(topic)
mqtt_push = threading.Thread(target=data_push.push_data_to_mosquitto)
kafka_push = threading.Thread(target=data_push.kafka_data)
mqtt_push.start()
kafka_push.start()