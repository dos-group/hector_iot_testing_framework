#!/usr/bin/python
from iot_api import *
import os
import sys
import time
import subprocess as sub
# Check if sudo
if os.geteuid() != 0:
    print("You need to have root privileges to run this script.\nPlease try again, this time using 'sudo'. Exiting.")
    sys.exit(1)

testbed = VirtualNetwork()

host_ip = sub.check_output("hostname -I | cut -d\' \' -f1", shell=True, universal_newlines=True).replace("\n", "")
print(str(host_ip))
testbed.add_hw_machine("192.168.0.239", "id_rsa", "julian", "192.168.0.39")


experiment = FlinkExperiment(testbed, "hospital_experiment_10node.properties")
print("Setting network properties")
#testbed.set_network_properties(em)
print("Starting Flink experiment")
experiment.start_experiment()

# Waiting for further actions
while(True):
    try:
        txt = input("").split(' ')
        # exit
        if(txt[0] == "exit"):
            break
        else:
            print("Unknown command!")
    except:
        pass

experiment.stop_experiment()
testbed.reset_network_properties()
experiment.turn_down()
