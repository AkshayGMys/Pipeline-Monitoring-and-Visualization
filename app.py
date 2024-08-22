import time
from coolingComponents import kafkaComp, mosquittoComp, logstashComp, opensearchComp
from flask import Flask, jsonify
import threading
from data import Logger, dataflow
import yaml

app = Flask(__name__)
log = Logger()
opensearch_status = "Inactive"
kafka_status = "Inactive"
mosquitto_status = "Inactive"
logstash_status = "Inactive"

opensearch_uptime = "0 days, 0 hours, 0 minutes, 0 seconds"
kafka_uptime = "0 days, 0 hours, 0 minutes, 0 seconds"
mosquitto_uptime = "0 days, 0 hours, 0 minutes, 0 seconds"
logstash_uptime = "0 days, 0 hours, 0 minutes, 0 seconds"

opensearch_node_color = "red"
kafka_node_color = "red"
mosquitto_node_color = "red"
logstash_node_color = "red"

mqtt_to_kafka_edge = "red"
kafka_to_logstash_edge = "red"
logstash_to_opensearch_edge = "red"

mqtt_to_kafka_mainStat = 0.0
kafka_to_log_mainStat = 0.0
log_to_open_mainStat = 0.0


@app.route('/api/graph/fields')
def fetch_graph_fields():
    nodes_fields = [{"field_name": "id", "type": "string"},
                    {"field_name": "title", "type": "string"},
                    {"field_name": "mainStat", "type": "string"},
                    {"field_name": "detail__role", "type": "string", "displayName": "Role"},
                    {"field_name": "detail__uptime", "type": "string", "displayName": "Uptime"},
                    {"field_name":"arc__failed","type":"number", "color":"red","displayName":"Inactive"},
                    {"field_name":"arc__passed","type":"number", "color":"green","displayName":"Active"},
                    {"field_name":"arc__neither","type":"number", "color":"yellow","displayName":"Active (No Dataflow)"}]
    edges_fields = [
        {"field_name": "id", "type": "string"},
        {"field_name": "source", "type": "string"},
        {"field_name": "target", "type": "string"},
        {"field_name": "mainStat", "type": "number"},
        {"field_name": "color", "type": "string"}
    ]
    result = {"nodes_fields": nodes_fields, "edges_fields": edges_fields}
    return jsonify(result)


@app.route('/api/graph/data')
def fetch_graph_data():
    topics_dict = dict()
    with open('/home/chandana96k/Downloads/HPE-CTY-2024-main/topic.yml', 'r') as file:
        topics_dict = yaml.safe_load(file)
    topics_list = topics_dict["kafka"]
    result = {"nodes":list([{"id": "pcim", "title": "PCIM", "arc__passed":1.0,"arc__failed":0.0,"arc__neither":0.0},{"id": "grafana", "title": "Grafana", "arc__passed":1.0,"arc__failed":0.0,"arc__neither":0.0}]),"edges":list()}
    for topic in topics_list:
        update_statuses(topic)
        mqtt_col = [0.0,0.0,0.0]
        kafka_col = [0.0,0.0,0.0]
        log_col = [0.0,0.0,0.0]
        open_col = [0.0,0.0,0.0]

        if kafka_node_color == "green":
            kafka_col[0] = 1.0
        elif kafka_node_color == "red":
            kafka_col[1] = 1.0
        else:
            kafka_col[2] = 1.0

        if mosquitto_node_color == "green":
            mqtt_col[0] = 1.0
        elif mosquitto_node_color == "red":
            mqtt_col[1] = 1.0
        else:
            mqtt_col[2] = 1.0

        if logstash_node_color == "green":
            log_col[0] = 1.0
        elif logstash_node_color == "red":
            log_col[1] = 1.0
        else:
            log_col[2] = 1.0

        if opensearch_node_color == "green":
            open_col[0] = 1.0
        elif opensearch_node_color == "red":
            open_col[1] = 1.0
        else:
            open_col[2] = 1.0

        result["nodes"] += [{"id": "mqtt_"+topic, "title": "Mosquitto_"+topic, "detail__role": "extrct(IOT)","mainStat": mosquitto_status , "detail__uptime":mosquitto_uptime,"arc__passed":mqtt_col[0],"arc__failed":mqtt_col[1],"arc__neither":mqtt_col[2]},
                {"id": "kafka_"+topic, "title": "Kafka_"+topic, "detail__role": "Stream","mainStat": kafka_status,"detail__uptime":kafka_uptime,"arc__passed":kafka_col[0],"arc__failed":kafka_col[1],"arc__neither":kafka_col[2]},
                {"id": "logstash_"+topic, "title": "Logstash_"+topic,  "detail__role": "Data Processing","mainStat": logstash_status,"detail__uptime": logstash_uptime,"arc__passed":log_col[0],"arc__failed":log_col[1],"arc__neither":log_col[2]},
                {"id": "opensearch_"+topic, "title": "Opensearch_"+topic,  "detail__role": "Database","mainStat": opensearch_status,"detail__uptime": opensearch_uptime,"arc__passed":open_col[0],"arc__failed":open_col[1],"arc__neither":open_col[2]}]
        result["edges"] += [{"id": "pcim_to_mqtt_"+topic, "source": "pcim", "target": "mqtt_"+topic, "mainStat": 1.0, "color": "green"},{"id": "mqtt_to_kafka_"+topic, "source": "mqtt_"+topic, "target": "kafka_"+topic, "mainStat": mqtt_to_kafka_mainStat, "color": mqtt_to_kafka_edge},
                {"id": "kafka_to_log"+topic, "source": "kafka_"+topic, "target": "logstash_"+topic, "mainStat": kafka_to_log_mainStat, "color": kafka_to_logstash_edge},
                {"id": "log_to_open_"+topic, "source": "logstash_"+topic, "target": "opensearch_"+topic, "mainStat": log_to_open_mainStat, "color": logstash_to_opensearch_edge}, {"id": "open_to_grafana_"+topic, "source": "opensearch_"+topic, "target": "grafana", "mainStat": 1.0, "color": "green"}]
    print(result)
    return jsonify(result)




@app.route('/api/health')
def check_health():
    return "API is working well!", 200

def update_statuses(topic):
    kafka_metric = kafkaComp()
    mosquitto_metric = mosquittoComp()
    logstash_metric = logstashComp()
    opensearch_metric = opensearchComp()
    data_pipeline = dataflow(topic)
    global kafka_status, mosquitto_status, logstash_status, opensearch_status
    global kafka_uptime,mosquitto_uptime,logstash_uptime, opensearch_uptime
    global kafka_node_color, mosquitto_node_color, logstash_node_color, opensearch_node_color
    global mqtt_to_kafka_edge, kafka_to_logstash_edge, logstash_to_opensearch_edge
    global mqtt_to_kafka_mainStat, kafka_to_log_mainStat, log_to_open_mainStat
    
    kafka_status = kafka_metric.get_comp_status()
    mosquitto_status = mosquitto_metric.get_comp_status()
    logstash_status = logstash_metric.get_comp_status()
    opensearch_status = opensearch_metric.get_comp_status()

    kafka_uptime = kafka_metric.get_service_uptime()
    mosquitto_uptime = mosquitto_metric.get_service_uptime()
    logstash_uptime = logstash_metric.get_service_uptime()
    opensearch_uptime = opensearch_metric.get_service_uptime()

    kafka_node_color = kafka_metric.node_color
    mosquitto_node_color = mosquitto_metric.node_color
    logstash_node_color = logstash_metric.node_color
    opensearch_node_color = opensearch_metric.node_color

    mqtt_to_kafka_edge = data_pipeline.check_kafka_data()
    kafka_to_logstash_edge = data_pipeline.kafka_logstash_dataflow()
    logstash_to_opensearch_edge = data_pipeline.monitor_index_growth()

    if mqtt_to_kafka_edge == "red" and kafka_node_color == "green":
        kafka_node_color = "yellow"

    if kafka_to_logstash_edge == "red" and logstash_node_color == "green":
        logstash_node_color = "yellow"

    if logstash_to_opensearch_edge == "red" and opensearch_node_color == "green":
        opensearch_node_color = "yellow"

    if mqtt_to_kafka_edge == "green":
        mqtt_to_kafka_mainStat = 1.0
    else:
        mqtt_to_kafka_mainStat = 0.0

    if kafka_to_logstash_edge == "green":
        kafka_to_log_mainStat = 1.0
    else:
        kafka_to_log_mainStat = 0.0

    if logstash_to_opensearch_edge == "green":
        log_to_open_mainStat = 1.0
    else:
        log_to_open_mainStat = 0.0        

if __name__ == '__main__':
    try:
        log.log_message("**Start**")
        log.log_message(app.run(host='127.0.0.1', port=5000, debug=True))
    except Exception as e:
        log.log_error(e.with_traceback)
        
    
    



