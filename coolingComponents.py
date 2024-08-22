from data import dataCollect

class kafkaComp(dataCollect):
    def __init__(self):
        super().__init__('kafka.service')
        
    def check_kafka_uptime(self):
        self.uptime = super().get_service_uptime()
        if self.uptime:
            super().log_message(f"Uptime of kafka: {self.uptime}")

class mosquittoComp(dataCollect):
    def __init__(self):
        super().__init__("mosquitto")

    def check_mosquitto_uptime(self):
        self.uptime = super().get_service_uptime()
        if self.uptime:
            super().log_message(f"Uptime of mosquitto: {self.uptime}")

class logstashComp(dataCollect):
    def __init__(self):
        super().__init__("logstash")

    def check_logstash_uptime(self):
        self.uptime = super().get_service_uptime()
        if self.uptime:
            super().log_message(f"Uptime of logstash: {self.uptime}")

class opensearchComp(dataCollect):
    def __init__(self):
        super().__init__("opensearch")

    def check_logstash_uptime(self):
        self.uptime = super().get_service_uptime()
        if self.uptime:
            super().log_message(f"Uptime of opensearch: {self.uptime}")