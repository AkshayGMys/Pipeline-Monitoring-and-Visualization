import argparse
import subprocess
import os
import requests

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
group.add_argument("-e","--enable",  help="enable the monitor service", action="store_true")
group.add_argument("-d","--disable",  help="disable the monitor service", action="store_true")
group.add_argument("-s","--status",  help="check the status of the monitor service", action="store_true")
group.add_argument("-st","--start", help="to start the monitor service", action="store_true")
group.add_argument("-sto","--stop", help= "to stop the monitor service", action="store_true")

args = parser.parse_args()

if args.enable:
    subprocess.run(['/usr/bin/systemctl', 'enable', 'monitor'], text= True)
if args.disable:
    subprocess.run(['/usr/bin/systemctl', 'disable', 'monitor'], text= True)
if args.start:
    subprocess.run(['/usr/bin/systemctl','start','monitor'], text= True)
if args.stop:
    subprocess.run(['/usr/bin/systemctl','stop','monitor'], text= True)
if args.status:
    subprocess.run(['/usr/bin/systemctl', 'status', 'monitor'], text=True)
    grafana_file = "/usr/bin/systemctl"
    path_exitsts = "Found" if os.path.exists(grafana_file) else "Not Found"
    print(f"Node graph API data source plugin: {path_exitsts}")
    tool_running = "" 
    try: 
        if requests.head("127.0.0.1:5000") == 200:
            tool_running = "Active [running]"
    except requests.exceptions.InvalidSchema:
        tool_running = "Inactive [not running]"
    print(f"Visualizer tool: {tool_running}")