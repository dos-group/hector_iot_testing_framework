# Héctor IoT Testing Framework
A framework designed to run experiments and application test on real and emulated IoT environments.
Paper to the code available here: https://dl.acm.org/doi/10.1145/3368235.3368832
### Supported platforms:
+ Apache Kafka
+ Apache Flink
+	Flink fork for edge

### OS-Dependencies:
+	Linux operating system (on all machines)
+	dnsmasq
+	qemu
+	qemu-system-arm (and others if required by virtual machine)
+	openssh (on all machines)
+	iproute2 (on all machines)
+	timesyncd (on all machines)
+	Recommended: ntp server in testbed topology

### Python-Dependencies:
+	Python 3.7
+	*requests* Python module

### Other dependencies:
+	Properly prepared VM images
+	Necessary files for qemu emulation (kernel, device tree)
+	Copy bridge.conf to /etc/qemu/ that includes "allow br0" (or whatever interface you want to use)

## How To
+ To use the framework import iot_api.py
+ Configure experiments using a .properties file and Python code (examples can be found in the framework folder)
+ For virtual testbeds, prepared disk images must be available (RSA key needs to be installed, JRE must be installed and if needed Apache Flink too)
+ More information on how to use the API can be found in the docs
+ Flink and Kafka folders are part of the project but can also be replaced by newer versions

## Future Work
+ Include automatic generation of VMs into NS3 setups
+ Add Microcontroller support
+ Add automatic setup of docker testbench
+ Flink data graphs to mirror topology
+ Add Kafka/Flink combinations, where Kafka producers run on VMs and Flink consumes on local host