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

vm1 = testbed.add_pi(img="../VMs/raspbian-stretch-lite.qcow")
vm2 = testbed.add_pi(img="../VMs/raspbian-stretch-lite.qcow")
vm3 = testbed.add_pi(img="../VMs/raspbian-stretch-lite.qcow")
vm4 = testbed.add_pi(img="../VMs/raspbian-stretch-lite.qcow")
vm5 = testbed.add_pi(img="../VMs/raspbian-stretch-lite.qcow")

testbed.add_hw_machine(device_ip, keyfile, username, host_ip)

testbed.run_machines()
for vm in testbed.vmList:
    vm.wait_for_connection()

############################################# Flink

em = {
    "delay": "100ms",
    "loss": "0.1"
}

experiment = FlinkExperiment(testbed, "hospital_experiment_10node.properties")
testbed.set_network_properties(em)
experiment.start_experiment()
time.sleep(10)
throughput = measure_throughput("../benchmark_results/networking/flink_net_1_throughput.csv", 60)
print(throughput)
experiment.stop_experiment()
testbed.reset_network_properties()
experiment.turn_down()


############################################################# Kafka

em = {
    "delay": "100ms",
    "loss": "0.1"
}

experiment = KafkaExperiment(testbed, "kafka_networking_experiment.properties")
testbed.set_network_properties(em)
experiment.start_experiment(outfile="../benchmark_results/networking/kafka_net_1_latency.csv")
time.sleep(10)
throughput = measure_throughput("../benchmark_results/networking/kafka_net_1_throughput.csv", 60)
print(throughput)
experiment.stop_experiment()
experiment.turn_down()

testbed.delete_network()
