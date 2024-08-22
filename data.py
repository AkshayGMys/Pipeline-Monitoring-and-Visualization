import subprocess
from datetime import datetime
import logging
import logging.handlers
import json
import paho.mqtt.client as mqtt
import random
from confluent_kafka import Consumer, KafkaError
import time
import requests
from pykafka import KafkaClient

kafka_previous_events_dict = {}
logstash_previous_events_dict = {}
opensearch_previous_events_dict = {}

class Logger:
    def __init__(self):
        logfileName = 'Logger.txt'
        logging_level = logging.DEBUG

        try:
            formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
            handler = logging.handlers.TimedRotatingFileHandler(logfileName, when="H", interval=10, backupCount=10)
            handler.setFormatter(formatter)
            self.logger = logging.getLogger()
            self.logger.addHandler(handler)
            self.logger.setLevel(logging_level)
        except Exception as e:
            self.logger.error(e.with_traceback)

    def log_message(self, message):
        self.logger.info(message)

    def log_error(self, errorMessage):
        self.logger.error(errorMessage)

    def log_alerts(self, warningMessage):
        self.logger.warning(warningMessage)

class dataCollect(Logger):
    def __init__(self, metric_name):
        super().__init__()
        self.metric_name = metric_name
        self.node_color = "red"
        self.status = "Inactive"

    def get_comp_status(self):
        try:
            output = subprocess.check_output(["/usr/bin/systemctl", "status",self.metric_name], stderr=subprocess.STDOUT, universal_newlines=True)
            if "active (running)" in output:
                self.node_color = "green"
                self.status = "Active"
                return "Active"
        except subprocess.CalledProcessError as e:
            super().log_error(f"Error: {e.output.strip()}")
            self.node_color = "red"
            self.status = "Inactive"
            return "Inactive"
        except Exception as e:
            super().log_error(e.with_traceback)
    
    def get_service_uptime(self):
        if self.status == "Inactive":
            return "0 days, 0 hours, 0 minutes, 0 seconds"
        try:
            self.output = subprocess.check_output(['/usr/bin/systemctl', 'show', '-p', 'ActiveEnterTimestamp', self.metric_name], text=True)
            self.active_enter_timestamp = self.output.strip()
            self.active_enter_time = datetime.strptime(self.active_enter_timestamp, 'ActiveEnterTimestamp=%a %Y-%m-%d %H:%M:%S %Z')
            self.uptime = datetime.now() - self.active_enter_time
            self.uptime_str = str(self.uptime.days) + " days, " + str(self.uptime.seconds // 3600) + " hours, " + str((self.uptime.seconds // 60) % 60) + " minutes, " + str(self.uptime.seconds % 60) + " seconds"
            return self.uptime_str
        except subprocess.CalledProcessError as e:
            super().log_error(f"Error: {e.with_traceback}")
            return None
        except Exception as e:
            super().log_error(e.with_traceback)

class dataflow(Logger):
    def __init__(self, topic):
        super().__init__()
        
        #Kafka configurations
        self.kafka_conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        }
        self.kafka_topic = topic

        # OpenSearch configuration
        self.es_host = 'localhost'
        self.es_port = 9200
        self.es_user = 'admin'
        self.es_password = 'Chin@123mayee'
        self.es_index = self.kafka_topic
        self.opensearch_endpoint = f"http://{self.es_host}:{self.es_port}"

        #Kafka previous events
        self.kafka_previous_events = 'kafka_previous_count.json' #create file
        self.kafka_previous_events_dict = self.load_previous_events(self.kafka_previous_events)

        #Logstash previous events
        self.logstash_previous_events = 'logstash_previous_count.json' #create file
        self.logstash_previous_events_dict = self.load_previous_events(self.logstash_previous_events)

        #Opensearch previous events
        self.opensearch_previous_events = 'opensearch_previous_count.json' #create file
        self.opensearch_previous_events_dict = self.load_previous_events(self.opensearch_previous_events)

    def load_previous_events(self,file):
        try:
            with open(file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_previous_events(self, file, dictionary):
        with open(file, 'w') as f:
            json.dump(dictionary, f)
    
    #Kafka dataflow
    def get_total_offset(self):
        command = f"/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group logstash --describe"
        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True, env={'PATH': '/usr/local/bin:/usr/bin:/bin:/opt/kafka/bin'})
            output = result.stdout.strip()
            #self.log_message(f"Kafka Consumer Group Command Output: {output}")

            lines = output.split('\n')
            for line in lines:
                if self.kafka_topic in line.split():
                    parts = line.split()
                    #self.log_message(f"Parsing line: {line}")
                    current_offset = parts[4]
                    #self.log_message(f"Current Offset: {current_offset}")
                    return int(current_offset)
            self.log_error(f"Failed to find the topic {self.kafka_topic} in the output.")
            return None

        except subprocess.CalledProcessError as e:
            self.log_error(f"CalledProcessError: {e}")
            return None
        except ValueError as e:
            self.log_error(f"ValueError: {e}")
            return None


    def check_kafka_data(self):
        current_offset = self.get_total_offset()
        previous_offset = self.kafka_previous_events_dict.get(self.kafka_topic,0)

        if(current_offset==None or previous_offset==None):
            return "red"
        
        if current_offset > previous_offset:
            self.kafka_previous_events_dict[self.kafka_topic] = current_offset
            self.save_previous_events(self.kafka_previous_events, self.kafka_previous_events_dict)
            self.log_message(f"kafka: current > previous: {self.kafka_topic, previous_offset, current_offset}")
            return "green"
        else:
            self.log_message(f"kafka: current <= previous: {self.kafka_topic, previous_offset, current_offset}")
            return "red"

    #Logstash dataflow
    def access_logstash_endpoint(self):
        url = "http://localhost:9600/_node/stats/pipelines"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
            return response
        except requests.exceptions.RequestException as e:
            self.log_error(f"Error accessing Logstash endpoint: {e}")
            return None

    def check_events_in(self):
        response = self.access_logstash_endpoint()
        if response and response.status_code == 200:
            data = response.json()
            current_events_in = data['pipelines'][self.kafka_topic]['events']['in']
            
            return current_events_in
        else:
            self.log_error("Failed to access Logstash monitoring API endpoint")
            return None

    def kafka_logstash_dataflow(self):
        try:
            current_events_in = self.check_events_in()
            previous_events_in = self.logstash_previous_events_dict.get(self.kafka_topic, 0)
            #self.log_message(f"Logstash events no: {self.kafka_topic, previous_events_in, current_events_in}")

            if(current_events_in==None or previous_events_in==None):
                #self.log_message(f"current events is none")
                return "red"

            elif current_events_in > previous_events_in:
                self.logstash_previous_events_dict[self.kafka_topic] = current_events_in
                self.save_previous_events(self.logstash_previous_events, self.logstash_previous_events_dict)
                #self.log_message(f"previous_events_in dictionary: {self.logstash_previous_events_dict[self.kafka_topic]}")
                self.log_message(f"Logstash: current > previous: {self.kafka_topic, previous_events_in, current_events_in}")
                return "green"
            
            elif current_events_in < previous_events_in:
                self.logstash_previous_events_dict[self.kafka_topic] = current_events_in
                self.save_previous_events(self.logstash_previous_events, self.logstash_previous_events_dict)
                self.log_message(f"Logstash: current < previous: {self.kafka_topic, previous_events_in, current_events_in}")
                return "red"

            else:
                self.log_message(f"Logstash: current = previous: {self.kafka_topic, previous_events_in, current_events_in}")
                return "red"

        except KeyboardInterrupt:
            pass


    #Opensearch dataflow
    def get_document_count(self):
        url = f"{self.opensearch_endpoint}/{self.es_index}/_count"
        auth = (self.es_user, self.es_password)

        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
            
            response_data = response.json()
            document_count = response_data.get("count", 0)
            return document_count

        except requests.exceptions.RequestException as e:
            self.log_error(f"Error accessing OpenSearch endpoint: {e}")
            return None


    def monitor_index_growth(self):
        current_document_count = self.get_document_count()
        previous_document_count = self.opensearch_previous_events_dict.get(self.kafka_topic, 0)
        #self.log_message(f"Logstash events no: {self.kafka_topic, previous_events_in, current_events_in}")

        if(current_document_count==None or previous_document_count==None):
            return "red"
        
        elif current_document_count > previous_document_count:
            self.opensearch_previous_events_dict[self.kafka_topic] = current_document_count
            self.save_previous_events(self.opensearch_previous_events, self.opensearch_previous_events_dict)
            #self.log_message(f"previous_events_in dictionary: {self.logstash_previous_events_dict[self.kafka_topic]}")
            self.log_message(f"Opensearch: current > previous: {self.kafka_topic, previous_document_count, current_document_count}")
            return "green"
        
        elif current_document_count < previous_document_count:
            self.opensearch_previous_events_dict[self.kafka_topic] = current_document_count
            self.save_previous_events(self.opensearch_previous_events, self.opensearch_previous_events_dict)
            #self.log_message(f"previous_events_in dictionary: {self.logstash_previous_events_dict[self.kafka_topic]}")
            self.log_message(f"Opensearch: current < previous: {self.kafka_topic, previous_document_count, current_document_count}")
            return "red"

        else:
            self.log_message(f"Opensearch: current = previous: {self.kafka_topic, previous_document_count, current_document_count}")
            return "red"
        
    
class push_data(Logger):
    def __init__(self, topic):
        super().__init__()
        self.topic = topic

    def push_data_to_mosquitto(self):
            try:
                broker_address = 'localhost'
                port = 1883
                client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                client.connect(broker_address, port)
                while True:
                    timestamp_ms = int(time.time() * 1000)       

                    #message_dict = {"name":"x9000cdu","timestamp":timestamp_ms,"device_type":"CCDU","Secondary_Pump_Differential_Pressure":random.randrange(18,20),"VFD1_RunTime_Energy_Counter":34397,"CDU_Voltage_Phase_2_3":0,"Primary_Facility_Supply_Water_Pressure":random.randrange(16,18),"VFD2_DC_Bus_Voltage":random.randrange(480,485),"Secondary_Cabinet_Supply_Water_Temperature":random.randrange(19,24),"Room_Dew_Point":random.randrange(2,5),"Relative_Humidity":random.randrange(28,32),"CDU_Voltage_Phase_3_1":0,"Secondary_Cabinet_Supply_Water_Pressure_2":32.2,"VFD1_Current":6.4,"CDU_Current_Phase_1":2.7,"Secondary_Cabinet_Return_Water_Temperature_2":25.1,"Secondary_Cabinet_Return_Water_Temperature":25.1,"PLC_to_VFD_Voltage":8.5,"Room_Temperature":22.4,"Primary_Facility_Return_Water_Temperature":23,"CDU_Current_Phase_3":0,"PLC_Temperature":37.7,"Secondary_Cabinet_Flow":16.9,"VFD1_DC_Bus_Voltage":489,"Secondary_System_Differential_Pressure":17.5,"Primary_Facility_Flow":5.5,"CWV_Valve_Actuator_Voltage":6.9,"Primary_Facility_Supply_Water_Temperature":11.5,"CDU_Voltage_Phase_1_2":210.5,"VFD1_Pump_Speed":3572,"VFD2_Current":5.5,"VFD2_Pump_Speed":3575,"Secondary_Cabinet_Supply_Water_Pressure":31.9,"Secondary_Pump_Suction_Pressure_2":13.6,"CDU_Current_Phase_2":0,"Secondary_Cabinet_Return_Water_Pressure_2":31,"Primary_Facility_Return_Water_Pressure":15.8,"CDU_Power":537,"Secondary_Cabinet_Supply_Water_Temperature_2":21.8,"Secondary_Cabinet_Return_Water_Pressure":31.1,"Secondary_Pump_Suction_Pressure_1":13.6,"Actuator_2_Feedback_Position":60,"VFD2_RunTime_Energy_Counter":34591}
                    message_dict = {"number":random.randrange(1,10),"timestamp":timestamp_ms}

                    message_json = json.dumps(message_dict)
                    print(message_json)    

                    client.publish(self.topic, message_json)
                    time.sleep(0.1)       

            except Exception as e:
                super().log_error(e.with_traceback)
                client.disconnect()

    def kafka_data(self):
            #MQTT settings
            mqtt_broker_host = "localhost"
            mqtt_topic = self.topic

            # Kafka settings
            kafka_broker = "localhost:9092"
            consumer_group = 'my_consumer_group2'

            # Setup Kafka client and producer
            kafka_client = KafkaClient(hosts=kafka_broker)
            kafka_topic = kafka_client.topics[self.topic.encode('utf-8')]
            kafka_producer = kafka_topic.get_producer()

            # Callback function to handle MQTT messages
            def on_message(client, userdata, msg):
                try:
                    # Decode the MQTT message payload
                    payload = json.loads(msg.payload.decode('utf-8'))
                    # Publish the data to Kafka
                    kafka_producer.produce(json.dumps(payload).encode('utf-8'))
                    print("kafka",payload)
            
                except Exception as e:
                    print("Error:", e)

            # Setup MQTT client
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            mqtt_client.on_message = on_message
            mqtt_client.connect(mqtt_broker_host)
            mqtt_client.subscribe(mqtt_topic)
            mqtt_client.loop_start()

            # Keep the script running
            try:
                while True:
                    pass
            except KeyboardInterrupt:
                mqtt_client.disconnect()
                kafka_producer.stop()
