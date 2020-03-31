# -*- coding: utf-8 -*-
"""API for running experiments in virtual IoT environments.

Note that localhost experiments cannot include virtual machines due to the communication setup between Kafka/Flink
clients. Usage needs root privileges and execution in a virtual environment is recommended (though not necessary).

Supported platforms:
	Apache Kafka
	Apache Flink
	Flink fork for edge

OS-Dependencies:
	Linux operating system (on all machines)
	dnsmasq
	qemu
	qemu-system-arm (and others if required by virtual machine)
	openssh (on all machines)
	iproute2 (on all machines)
	timesyncd (on all machines)
	Recommended: ntp server in testbed topology
Python-Dependencies:
	Python 3.7
	requests Python module

Other dependencies:
	Properly prepared VM images
	Necessary files for qemu emulation (kernel, device tree)
	Copy bridge.conf to /etc/qemu/ that includes "allow br0" (or whatever interface you want to use)


Ilja Behnke: i.behnke@tu-berlin.de

"""
import random                   # Needed for the generation of random MAC addresses.
import subprocess as sub        # Needed for running shell commands
import time                     # Needed for sleep()
from shutil import copyfile     # Needed to copy config files and VM images
import sys                      # Needed for exit()
import os                       # Needed at a lot of places for OS-Commands
import re                       # Needed for regular expression search on Flink job IDs
import configparser             # Needed to parse experiment configuration file
import requests                 # Needed to check job and taskmanager availability via Flink REST API
import json                     # Needed to check job and taskmanager availability via Flink REST API

SSH_STRING = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile /dev/null", "-o", "LogLevel ERROR"]
SCP_STRING = ["scp", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile /dev/null", "-o", "LogLevel ERROR"]

class VirtualNetwork:
	"""Class that represents the virtual network experiments run on.

	Construction and destruction of objects call the appropriate os commands. The backbone of a network always is a
	Linux bridge interface. This interface is given an IP address and a name.
	"""
	def __init__(self, bridge_ip='10.10.1.1', dev_name='br0'):
		"""Constructor for experiment network.

		:param bridge_ip: IP of bridge interface as string not containing the netmask (which is always /24)
		:param dev_name: Name of the Linux bridge interface to be created

		Additionally, this class includes a list of virtual machines that are part of the testbed and the number of
		clients (i.e. producers/taskmanagers) that runs on localhost. EMProperties is an options dictionary to be used
		with netem option/parameter pairs. The network properties are only emulated when calling
		set_network_properties(network, property_dict).
		Once a network object is turned down it cannot be reused and waits for garbage collection. At that point all
		os resources have been freed.

		Needs root privileges.

		"""
		print("Setting up bridged network")
		try:
			sub.run(["ip", "link", "add", "name", dev_name, "type", "bridge"])
			sub.run(["ip", "link", "set", dev_name, "up"])
			sub.run(["ip", "addr", "add", "dev", dev_name, bridge_ip+'/24'])

		except sub.CalledProcessError as err:
			print('ERROR: Bridge setup unsuccessful; ', err)
			sys.exit(2)
		self.bridge_ip = bridge_ip
		self.vmList = []
		self.lhClients = 0
		self.hwList = []
		self.devName = dev_name
		self.EMProperties = {
			"delay": "0 0 0",
			"delayDistribution": "normal",
			"packetLoss": "0",
			"packetDuplication": "0",
			"packetCorruption": "0"
		}
		self.emulateNetwork = False
		self.turned_down = False

	def add_vm(self, image_path, keyfile, username, system, kernel_path="", cpu="", qemu_options="", copy_vm=True):
		"""Creates a virtual machine, adds it to the network and assignes it an IP/MAC pair.

		:param image_path: Path to os image to be used by VM
		:param keyfile: The (RSA) key to be used to connect to the VM
		:param username: User on VM who is to run experiments
		:param system: System to be emulated by Qemu
		:param kernel_path: Path to kernel to be used by VM
		:param cpu: CPU to be emulated by Qemu
		:param qemu_options: Additional Qemu options such as kernel flags or device trees
		:return: VirtualMachine object

		A blue print to be copied is needed. Virtual machines have to be prepared before they can be used as blueprints.
		The necessary Flink/Kafka binaries have to be found at /home/user/Flink and /home/user/Kafka respectively.
		Depending on machine restrictions, property and configuration files have to be adjusted.

		Needs root privileges.

		"""
		if os.geteuid() != 0: # Check if sudo
			print("You need to have root privileges to run this script.\nPlease try again, this time using 'sudo'. Exiting.")
			sys.exit(1)
		mac = rand_mac()
		x = len(self.vmList)
		ip = '10.10.1.{:d}'.format(x+10)
		if copy_vm:
			hda = "virtualMachine_" + str(x) + ".qcow"
			copyfile(image_path, hda)
		else:
			hda = image_path
		vm = VirtualMachine(mac, ip, username, keyfile, hda, kernel_path, system, cpu, qemu_options, copy_vm)
		self.vmList.append(vm)
		return vm

	def add_pi(self, img="raspbian-stretch-lite.qcow"):
		"""Uses the add_vm method to add a Raspberry Pi VM to the network.

		:return: VirtualMachine object with Raspberry Pi parameters.

		Needs root privileges.

		"""
		return self.add_vm(img, "id_rsa", "pi", "qemu-system-arm", "kernel-qemu-4.14.79-stretch",
							"arm1176", '-append "root=/dev/sda2 panic=1 rootfstype=ext4 rw" -m 256 -M versatilepb') #-dtb versatile-pb.dtb

	def add_x86_64_arch(self):
		"""Uses the add_vm method to add a x86_64 VM running Arch Linux to the network.

		:return: VirtualMachine object with parameters.

		Needs root privileges.

		"""
		return self.add_vm("arch_hd.qcow2", "id_rsa", "user", "qemu-system-x86_64", qemu_options="-m 128M")

	def add_localhost_client(self, number):
		"""Adds a number of local host clients to the testbed.

		:param number: Number of clients to be added
		"""
		self.lhClients += number

	def add_hw_machine(self, ip, keyfile, username, host_ip):
		"""Adds a machine in the network to the testbed of type VirtualNetwork and sets its time via NTP

		:param ip: The reachable IP of the machine in the network
		:param keyfile: RSA key to connect via SSH
		:param username: User to run client on
		:param iface: Network interface of local host via which the machine is reachable

		Machines have to be running and an ssh service has to be running.

		"""
		pm = PhysicalMachine(ip, username, keyfile)
		try:
			sub.check_output(SSH_STRING + ["-q", "-i", keyfile, username + "@" + ip, "exit"])
			if len(self.hwList) == 0:
				sub.run("echo 1 > /proc/sys/net/ipv4/ip_forward", shell=True)
			self.hwList.append(pm)
			sub.run(SSH_STRING + ["-q", "-i", keyfile, username + "@" + ip, "sudo", "ip", "route", "add", "default", "via", host_ip])
			print("Added physical machine " + username + "@" + ip)
		except sub.CalledProcessError:
			print("Physical machine with IP ", ip, " could not be reached and will not be added to testbed")
		sub.run(SSH_STRING + ["-q", "-i", keyfile, username + "@" + ip, "sudo", "systemctl", "restart", "systemd-timesyncd"])
		return pm

	def shutdown_hw_machines(self):
		for pm in self.hwList:
			pm.shutdown()
		self.hwList.clear()

	def run_machines(self):
		"""Starts all virtual machines that have been added to the testbed.

		dnsmasq is used to manage IPs of virtual machines.

		Needs root privileges.

		"""
		dhcp_command = "dnsmasq --conf-file=dnsmasq.conf"
		for vm in self.vmList:
			print("Mapping",vm.mac ,"to", vm.ip)
			mapping=" --dhcp-host="+vm.mac + "," + vm.ip
			dhcp_command+=mapping
		print("Starting DHCP server: " + dhcp_command)
		try:
			sub.Popen(dhcp_command.split(" "))
		except sub.CalledProcessError as err:
			print('ERROR: dnsmasq could not be started; vm images are being removed', err)
			for vm in self.vmList:
				sub.run(["rm",vm.hda])
			del self.vmList[0:]
			sys.exit(2)
		for vm in self.vmList:
			print("Starting VM " + vm.username + "@" + vm.ip)
			qemu_command = vm.system + ' '
			if vm.kernel != "":
				qemu_command += ('-kernel ' + vm.kernel + ' ')
			qemu_command += ('-hda ' + vm.image_file + ' ')
			if vm.cpu != "":
				qemu_command += ('-cpu ' + vm.cpu + ' ')
			if vm.qemu_options != "":
				qemu_command += (vm.qemu_options + ' ')
			qemu_command += ('-no-reboot -daemonize -display none -net nic,macaddr=' + vm.mac + ' -net bridge,br=br0 &>qemu.log')
			os.system(qemu_command)
			# os.system(vm.system + ' \
			# -kernel '+ vm.kernel + ' \
			# -hda ' + vm.image_file +' \
			# -cpu ' + vm.cpu + ' ' + vm.qemu_options + ' \
			# -no-reboot \
			# -daemonize \
			# -display none \
			# -net nic,macaddr=' + vm.mac + ' -net bridge,br=br0 &>/dev/null')

	def set_network_properties(self, property_dict):
		"""Function to set the emulated properties of a VirtualNetwork on all virtual machines.

		This function enables shapeing of traffic on the virtual network bridge.
		This means that all passing traffic of all virtual and physical machines
		is subject to the shaping. If a heterogeneous setup is required, use the
		set_interface_properties() functios of VirtualMachine objects.

		:param property_dict: Dictionary of option/value pairs of netem command (see netem manual)

		Needs root privileges.

		"""
		# self.EMProperties = property_dict
		# command = ["tc", "qdisc", "add", "dev", self.devName, "root", "netem"]
		# for key in self.EMProperties:
		# 	command.append(key)
		# 	command += (self.EMProperties[key].split(" "))
		# sub.run(command)
		self.emulateNetwork = True
		for vm in self.vmList:
			vm.set_interface_properties(property_dict)

	def reset_network_properties(self):
		"""Deletes traffic shaping by netem

		Needs root privileges.

		"""
		# sub.run(["tc", "qdisc", "del", "dev", self.devName, "root", "netem"])
		for vm in self.vmList:
			if vm.traffic_shaped:
				vm.reset_interface()
		sub.run(["modprobe", "-r", "ifb"])

	def delete_network(self):
		"""Use to turn down network and free resources.

		All virtual machines in the testbed are powered down and deleted. Virtual network resources are freed.

		Needs root privileges.

		"""
		vm_mac = self.vmList[0].mac if len(self.vmList) > 0 else ""
		for vm in self.vmList:
			name = vm.username + "@" + vm.ip
			sub.Popen(SSH_STRING + ["-i", vm.key, name, "sudo", "shutdown", "-h", "now"])
			pid = get_pid(grep="qemu", args="macaddr=" + vm.mac)
			print("Shutting down VM " + name + " and its process with pid " + str(pid))
			sub.run(["kill", str(pid)])
		self.reset_network_properties()
		time.sleep(10)
		for x in range(len(self.vmList)):
			hda = self.vmList[x].image_file
			if self.vmList[x].copy_vm:
				sub.run(["rm", hda])
		self.vmList.clear()
		sub.run(["ip", "link", "delete", self.devName])
		if vm_mac != "":
			pid = get_pid(grep="dnsmasq", args="dhcp-host=" + vm_mac)
			print("Killing dnsmasq with pid " + str(pid))
			sub.run(["kill", str(pid)])
		else:
			print("Couldn't find started dnsmasq, pkill all dnsmasq processes")
			sub.run(["pkill", "dnsmasq"])
		try:
			os.remove("/var/lib/misc/dnsmasq.leases")
		except:
			"No dhcp leases to delete"
		self.turned_down = True

	def __del__(self):
		"""Frees and deletes all testbed resources in case of an unexpected error.

		Does the same as delete_network() and is skipped if resources are already free.
		Attention: Deletes all dnsmasq leases in /var/lib/misc/dnsmasq.leases

		Needs root privileges.
		"""
		if not self.turned_down:
			print("Deconstructing " + self.devName)
			self.delete_network()


class VirtualMachine:
	"""Class representing a virtual machine in a testbed.

	"""
	def __init__(self, mac, ip, username, keyfile, image_file, kernel, system, cpu, qemu_options, copy_vm):
		"""Initializes parameters of a VirtualMachine instance.

		:param mac: MAC address of VM
		:param ip: IP address that is linked to MAC by dhcp server (dnsmasq)
		:param username: User on VM-os to run client
		:param keyfile: Key (RSA) for connection to VM
		:param image_file: Image of the VM instance (will be deleted upon turn down of network)
		:param kernel: Kernel of VM
		:param system: System to be emulated by Qemu
		:param cpu: CPU to be emulated by Qemu
		:param qemu_options: Additional options in Qemu command
		:param copy_vm: Should the VM be run on the original file (False) or on a copy (True)

		"""
		self.mac = mac
		self.username = username
		self.ip = ip
		self.key = keyfile
		self.image_file = image_file
		self.kernel = kernel
		self.qemu_options = qemu_options
		self.system = system
		self.cpu = cpu
		self.traffic_shaped = False
		self.copy_vm = copy_vm

	def wait_for_connection(self):
		"""Wait until the VM can be reached via SSH and sets its clock via ntp

		Exits experiment on timeout.

		"""
		timeout = 0
		time.sleep(10)
		while True:
			try:
				sub.check_output(SSH_STRING + ["-q", "-i", self.key, self.username + "@" + self.ip, "exit"])
				sub.run(SSH_STRING + ["-q", "-i", self.key, self.username + "@" + self.ip, "sudo", "systemctl", "restart", "systemd-timesyncd"])
				print("ssh connection to: " +str(self.ip) + " successful!")
				return 0
			except sub.CalledProcessError:
				if timeout > 50:
					print("VM could not be reached; exiting")
					sys.exit(3)
				time.sleep(2)
				timeout += 1

	def upload_script(self, file, dest_folder="/home/pi", execute=True):
		"""Starts a script in a folder of the VM. Does not wait until execution of script is finished.

		:param file: File to be uploaded to VM
		:param dest_folder: Destination on VM

		"""
		sub.run(SCP_STRING + ["-i", self.key, file, self.username + "@" + self.ip + ":" + dest_folder])
		if execute:
			sub.Popen(SSH_STRING + ["-i", self.key, self.username + "@" + self.ip + ":" + dest_folder + "/script.sh"])

	def change_interface_properties(self, property_dict, interface=["tap", "ifb"]):
		"""Used to change traffic settings for this VM only.

		:param property_dict: Dictionary of netem properties (option:value(s))

		"""
		interface = interface.copy()
		id = int(self.ip.split(".")[-1])-10
		if not self.traffic_shaped:
			print("Error: Traffic of device {:d} NOT emulated.".format(id))
			return
		if(interface != None and len(interface) > 0):
			for idx in range(len(interface)):
				interface[idx] += "{:d}".format(id)
				#print(interface[idx])
		else:
			print("Empty interface list or None!")
			return
		command = "tc qdisc change dev _interface_ root netem"
		for key, value in property_dict.items():
			command += " " + str(key) + " " + str(value)
		for idx in range(len(interface)):
			comm = command.replace("_interface_", interface[idx], 1)
			sub.Popen(comm, shell=True)
			print("Executing command: " + comm)

	def set_interface_properties(self, property_dict):
		"""Used to shape traffic for this VM only.

		:param property_dict: Dictionary of netem properties (option:value(s))

		"""

		id = int(self.ip.split(".")[-1])-10
		if self.traffic_shaped:
			print("Error: Traffic of device {:d} already emulated.".format(id))
			return
		interface = "tap{:d}".format(id)
		ifb_device = "ifb{:d}".format(id)
		command = ["tc", "qdisc", "add", "dev", interface, "root", "netem"]
		for key in property_dict:
			command.append(key)
			command += (property_dict[key].split(" "))
		print("TC for " + self.username + "@" + str(self.ip) + " > ", end = '')
		print(*command, sep = ", ")
		sub.run(command)
		#Create intermediate functional block device to shape incoming traffic
		sub.run(["modprobe", "ifb"])
		if id > 1:
			sub.run(["ip", "link", "add", "name", ifb_device, "type", "ifb"])
		sub.run(["ip", "link", "set", "dev", ifb_device, "up"])
		sub.run(["tc", "qdisc", "add", "dev", interface, "ingress"])
		sub.run(["tc", "filter", "add", "dev", interface, "parent", "ffff:", "protocol", "ip", "u32", "match", "u32", "0", "0", "flowid", "1:1", "action", "mirred", "egress", "redirect", "dev", ifb_device])
		command = ["tc", "qdisc", "add", "dev", ifb_device, "root", "netem"]
		for key in property_dict:
			command.append(key)
			command += (property_dict[key].split(" "))
		sub.run(command)
		self.traffic_shaped = True

	def reset_interface(self):
		"""Remove traffic shaper from VM interface.

		"""
		id = int(self.ip.split(".")[-1])-10
		ifb_device = "ifb{:d}".format(id)
		interface = "tap{:d}".format(id)

		#print("Deleting traffic control settings and linked IPs")
		sub.run(["tc", "qdisc", "del", "dev", ifb_device, "root", "netem"], stderr=sub.DEVNULL)
		sub.run(["tc", "filter", "del", "dev", interface], stderr=sub.DEVNULL)
		sub.run(["tc", "qdisc", "del", "dev", interface, "ingress"], stderr=sub.DEVNULL)
		sub.run(["ip", "link", "del", "dev", ifb_device], stderr=sub.DEVNULL)
		sub.run(["tc", "qdisc", "del", "dev", interface, "root", "netem"], stderr=sub.DEVNULL)
		
		self.traffic_shaped = False

class PhysicalMachine:
	"""Class representing a physical machine in the network.

	"""
	def __init__(self, ip, username, keyfile):
		"""Initializes parameters of a VirtualMachine instance.

		:param ip: IP address of machine in network
		:param username: User on machine to run client
		:param keyfile: Key (RSA) for connection to machine

		"""
		self.username = username
		self.ip = ip
		self.key = keyfile

	def shutdown(self):
		"""Halts the machine.

		"""
		sub.run(SSH_STRING + ["-i", self.key, self.username + "@" + self.ip, "sudo", "shutdown", "-h", "now"])

	def run_script(self, file, dest_folder="/home/pi", execute=True):
		"""Starts a script in a folder of the VM. Does not wait until execution of script is finished.

		:param file: File to be uploaded to VM
		:param dest_folder: Destination on VM

		"""
		sub.run(SCP_STRING + ["-i", self.key, file, self.username + "@" + self.ip + ":" +dest_folder + "/script.sh"])
		if execute:
			sub.Popen(SSH_STRING + ["-i", self.key, self.username + "@" + self.ip + ":" + dest_folder + "/script.sh"])


class KafkaExperiment:
	"""Represents a Kafka setup including all parameters needed to start a Kafka server.

	"""

	def __init__(self, network, config_file):
		"""Initializes Kafka properties and sets up a Kafka experiment.

		:param network: VirtualNetwork testbed to run on.
		:param config_file: Properties file of experiment (see API documentation)

		"""

		config = configparser.ConfigParser()  # Parse configuration
		config.read(config_file)
		properties = config['KafkaExperiment']
		kafka_path = properties['kafkaPath']
		data_dir = properties['datadir']
		client_port = properties['clientPort']
		max_client_cnxns = properties['maxClientCnxns']
		broker_properties = properties['brokerProperties'].strip("[] ").split(";")
		start_stream_app = properties.getboolean('startStreamApp')
		app_path = properties['appPath']
		app_arguments = properties['appArguments']
		topics = properties['topics'].strip('[] ').split(';')
		topic_array = []
		i = 0
		for topic_string in topics:
			element = topic_string.strip('()').split(',')
			topic_array.append(element)
			i = i+1
		producer_path = properties['producerPath']
		producer_args = properties['producerArgs']
		producers_per_client = properties.getint('producersPerClient')

		start_zookeeper(kafka_path, data_dir, client_port, max_client_cnxns)
		time.sleep(5)
		kafkalog = open("kafka.log", "w+")
		# Start brokers
		for config in broker_properties:
			broker = sub.Popen([kafka_path + 'bin/kafka-server-start.sh', config], stdout=kafkalog)
			print("Broker started with pid " + str(broker.pid))
			time.sleep(10)
		# Transfer producer apps
		print("Transferring producer apps...")
		for vm in network.vmList:
			sub.run(SCP_STRING + ["-i", vm.key, producer_path, vm.username + "@"+vm.ip+":/home/" + vm.username + "/producer.jar"])
		for pm in network.hwList:
			sub.run(SCP_STRING + ["-i", pm.key, producer_path, pm.username + "@"+pm.ip+":/home/" + pm.username + "/producer.jar"])
		# Create topics
		print("Create topics...")
		for tripel in topic_array:
			os.system(kafka_path + 'bin/kafka-topics.sh --create --zookeeper localhost:' + client_port +
													' --replication-factor ' + tripel[1] + ' --partitions ' + tripel[2] + ' --topic ' + tripel[0])
			time.sleep(2)

		self.kafka_path = kafka_path
		self.data_dir = data_dir
		self.client_port = client_port
		self.max_client_cnxns = max_client_cnxns
		self.broker_properties = broker_properties
		self.producer_path = producer_path
		self.producer_args = producer_args
		self.producers_per_client = producers_per_client
		self.topic_array = topic_array
		self.start_stream_app = start_stream_app
		self.app_path = app_path
		self.app_arguments = app_arguments
		self.turned_down = False
		self.lh_producers = []
		self.streams_tuple = (None, None)
		self.network = network


	def start_experiment(self, outfile=None):
		"""Start all Kafka producers in the testbed and the Kafka Streams app if configured

		:param network: VirtualNetwork testbed
		:param experiment: KafkaProperties experiment
		:param outfile: File to write output to or None if stdout should print to shell

		"""
		app = None
		file = None
		if self.start_stream_app:
			print("Starting Streams Application!")
			if outfile != None:
				file = open(outfile,"w")
				app = sub.Popen(["java", "-jar", self.app_path] + self.app_arguments.split(" "), stdout=file)
			else:
				app = sub.Popen(["java", "-jar", self.app_path] + self.app_arguments.split(" "))
			self.set_streams_context((app, file))
		for vm in self.network.vmList:
			producer_dir = vm.username + "@" + vm.ip
			for x in range(self.producers_per_client):
				sub.Popen(SSH_STRING + ["-i", vm.key, producer_dir, "java", "-jar", "producer.jar"] + self.producer_args.split(" "))
		for x in range(self.network.lhClients * self.producers_per_client):
			self.lh_producers[x] = sub.Popen(["java", "-jar", self.producer_path] + self.producer_args.split(" "))
		for pm in self.network.hwList:
			producer_dir = pm.username + "@" + pm.ip
			for x in range(self.producers_per_client):
				sub.Popen(SSH_STRING + ["-i", pm.key, producer_dir, "java", "-jar", "producer.jar"] + self.producer_args.split(" "))
		return (app, file)

	def stop_experiment(self):
		"""Stop running Kafka experiment by killing all producers and the Kafka Streams app if one was started.

		Note: Currently kills all Java processes on client machines. This could be changed...

		"""
		for vm in self.network.vmList:
			sub.run(SSH_STRING + ["-i", vm.key, vm.username + "@" + vm.ip, "sudo", "pkill", "java"])
		for pm in self.network.hwList:
			sub.run(SSH_STRING + ["-i", pm.key, pm.username + "@" + pm.ip, "sudo", "pkill", "java"])
		for lh_producer in self.lh_producers:
			lh_producer.terminate()
		if self.streams_tuple[0] != None:
			self.streams_tuple[0].terminate()
			if self.streams_tuple[1] != None:
				self.streams_tuple[1].flush()
				self.streams_tuple[1].close()
		time.sleep(5)
		for vm in self.network.vmList:
			producer_dir = vm.username + "@" + vm.ip
			sub.Popen(SSH_STRING + ["-i", vm.key, producer_dir, "rm", "producer.jar"])

	def turn_off_vms(self):
		"""Only turn off producers while keeping the brokers and consumers running. Turns off VMs

		Deprecated.
		"""
		for vm in self.network.vmList:
			sub.run(SSH_STRING + ["-i", vm.key, vm.username + "@" + vm.ip, "pkill", "producer"])
			sub.run(SSH_STRING + ["-i", vm.key, vm.username + "@" + vm.ip, "sudo", "shutdown", "-h", "now"])

	def set_streams_context(self, streams_tuple):
		"""Sets context of started Kafka Streams App

		:param streams_tuple: A (Subprocess, File) tuple generated by kafka_start_experiment()

		"""
		self.streams_tuple = streams_tuple

	def turn_down(self):
		"""Turns down Kafka cluster.

		Note: Kills all Java applications.

		"""
		os.remove('zkConfig.temp')
		for tripel in self.topic_array:
			sub.run([self.kafka_path + 'bin/kafka-topics.sh', '--delete', '--zookeeper', 'localhost:' + self.client_port, '--topic', tripel[0]])
			time.sleep(2)
		sub.run([self.kafka_path + 'bin/kafka-server-stop.sh'])
		time.sleep(3)
		sub.run([self.kafka_path + 'bin/zookeeper-server-stop.sh'])
		time.sleep(3)
		sub.run(["pkill", "java"])
		self.turned_down = True

	def __del__(self):
		if not self.turned_down:
			print("Deconstructing Kafka experiment")
			self.turn_down()


class FlinkExperiment:
	"""Represents Flink experiments.

	"""
	def __init__(self, network, config_file):
		"""Configure Flink experiment and set up master and slave machines.

		:param network: VirtualNetwork experiment
		:param config_file: Experiment configuration (see API documentation)
		:return: FlinkProperties instance containing all necessary experiment parameters

		Starts jobmanager and taskmanagers. Waits until everything is ready.

		"""
		# Parse configuration
		config = configparser.ConfigParser()
		config.read(config_file)
		self.properties = config['FlinkExperiment']
		flink_dir = self.properties['flinkDir']
		flink_conf = self.properties['flinkConf']
		app_path = self.properties['appPath']
		app_arguments = self.properties['appArguments']
		flink_for_edge = self.properties.getboolean('flinkForEdge')

		print("Copying Flink configuration")
		try:
			copyfile(flink_dir+"/conf/flink-conf.yaml", flink_dir+"/conf/flink-conf.yaml.save")
		except FileNotFoundError:
			print("No config file to store")
		try:
			copyfile(flink_conf, flink_dir+"/conf/flink-conf.yaml")
		except FileNotFoundError:
			print("Config File not found, using default Flink config")
		print("Starting Jobmanager")
		if flink_for_edge:
			sub.Popen([flink_dir+"/bin/jobmanager.sh", "start", "cluster"])
		else:
			sub.Popen([flink_dir+"/bin/jobmanager.sh", "start"])
		code = 0
		while code != 200:
			try:
				response = requests.get("http://" + network.bridge_ip + ":8081/overview")
				code = response.status_code
				time.sleep(2)
			except:
				time.sleep(2)
		print("Starting Taskmanagers")
		if flink_for_edge:
			tm_path = "flink-for-edge/bin/taskmanager.sh"
		else:
			tm_path = "Flink/bin/taskmanager.sh"
		for pm in network.hwList:
			name = pm.username + "@" + pm.ip
			sub.Popen(SSH_STRING + ["-i", pm.key, name, tm_path, "start"])
		for vm in network.vmList:
			name = vm.username + "@" + vm.ip
			sub.Popen(SSH_STRING + ["-i", vm.key, name, tm_path, "start"]) #start-foreground
		for tm in range(network.lhClients):
			sub.run([flink_dir+"/bin/taskmanager.sh", "start"])

		self.flink_dir = flink_dir
		self.flink_conf = flink_conf
		self.app_path = app_path
		self.app_arguments = app_arguments
		self.turned_down = False
		self.flink_for_edge = flink_for_edge
		self.network = network

		print("Waiting for taskmanagers")
		while True:
			response = requests.get("http://" + network.bridge_ip + ":8081/overview")
			tm = json.loads(response.text)['taskmanagers']
			#print( str(tm) + " / " + str(len(network.vmList) + network.lhClients + len(network.hwList)) )
			if tm == (len(network.vmList) + network.lhClients + len(network.hwList)):
				return
			time.sleep(3)
		print("Flink Experiment startet on host")

	def start_experiment(self):
		"""Runs a previously set up Flink experiment by uploading the task to the jobmanager.

		The default parallelization is automatically set to the number of taskmanagers in the testbed.

		:param network: VirtualNetwork testbed
		:param experiment: FlinkProperties experiment

		"""
		print("Starting distributed application.")
		sub.Popen([self.flink_dir + "/bin/flink", "run", "-p", str(len(self.network.vmList) + self.network.lhClients + len(self.network.hwList)),
					self.app_path] + self.app_arguments.split(" "))


	def stop_experiment(self):
		"""Stops the execution of the distributed task on the IoT testbed.

		Cancels all running Flink jobs and stops all taskmanagers and the jobmanager.

		"""
		string = str(sub.run([self.flink_dir+"/bin/flink", "list"])) #str(sub.run([self.flink_dir+"/bin/flink", "list"], capture_output=True))
		pattern = "[0-9a-f]{32}"
		jobs = re.findall(pattern, string)
		if self.flink_for_edge:
			tm_path = "flink-for-edge/bin/taskmanager.sh"
		else:
			tm_path = "Flink/bin/taskmanager.sh"
		for job in jobs:
			sub.run([self.flink_dir+"/bin/flink", "cancel", job])
		for pm in self.network.hwList:
			name = pm.username + "@" + pm.ip
			sub.Popen(SSH_STRING + ["-i", pm.key, name, tm_path, "stop"])
		for vm in self.network.vmList:
			name = vm.username + "@" + vm.ip
			sub.Popen(SSH_STRING + ["-i", vm.key, name, tm_path, "stop"])
		sub.run([self.flink_dir+"/bin/taskmanager.sh", "stop-all"])
		time.sleep(6)
		sub.run([self.flink_dir + "/bin/jobmanager.sh", "stop"])
		time.sleep(6)


	def wait_for_jobs(self):
		"""Waits until all jobs in the current experiment setup have finished.

		:return: Time taken resolution 1 second

		"""
		running = True
		start = time.time()
		while running:
			response = requests.get("http://" + self.network.bridge_ip + ":8081/jobs")
			status = json.loads(response.text)['jobs'][0]['status']
			if status != 'RUNNING':
				running = False
			time.sleep(1)
		return time.time()-start

	def turn_down(self):
		"""Turns down a running Flink experiment by cancelling all jobs and stopping job and taskmanagers.
		   Does not stop taskmanagers on physical machines!
		"""
		string = str(sub.run([self.flink_dir + "/bin/flink", "list"])) #str(sub.run([self.flink_dir + "/bin/flink", "list"], capture_output=True))
		pattern = "[0-9a-f]{32}"
		jobs = re.findall(pattern,string)
		for job in jobs:
			sub.run([self.flink_dir + "/bin/flink", "cancel", job])
		sub.run([self.flink_dir + "/bin/taskmanager.sh", "stop-all"])
		sub.run([self.flink_dir + "/bin/jobmanager.sh", "stop"])
		print("Restoring Flink configuration")
		try:
			copyfile(self.flink_dir+"/conf/flink-conf.yaml.save", self.flink_dir+"/conf/flink-conf.yaml")
			os.remove(self.flink_dir+"/conf/flink-conf.yaml.save")
		except FileNotFoundError:
			print("No config file to restore")
			os.remove(self.flink_dir+"/conf/flink-conf.yaml")
		self.turned_down = True

	def get_all_taskslots(self, network, mode='all'):
		""" Returns all TaskSlots for this Flink experiment from all in the network existing Taskmanagers
		
		:param network: VirtualNetwork object
		:param mode: 'all' returns all existing TaskSlots, 'free' returns number of free TaskSlots (may be negative if more needed than available)
		
		"""
		slots = 0
		for vm in network.vmList:
			multiplicator = 1
			ssh = ["-q", "-i", vm.key, vm.username + "@" + vm.ip]
			comm = ssh + ["cat", "Flink/conf/flink-conf.yaml", "|", "grep", "-i", "taskmanager.numberOfTaskSlots:"]
			try:
				# Get number of running TaskManagers on this machine
				pgrep = ssh + ["pgrep", "java", "-a", "|", "grep", "TaskManagerRunner"]
				procs = sub.check_output(SSH_STRING + pgrep, universal_newlines=True)
				print("TaskManagers running on " + vm.username + "@" + vm.ip + " " + str(len(procs.split("\n"))-1))
				multiplicator = len(procs.split("\n"))-1
			except Exception as ex:
				# No TaskManager running on this machine => continue
				print(ex)
				continue
			slots += multiplicator * int(sub.check_output(SSH_STRING + comm, universal_newlines=True).replace("taskmanager.numberOfTaskSlots: ", "").replace("\n", ""))
		#TODO do same for hwList and lhClients
		if(mode == 'free'):
			pass
		return slots

	def __del__(self):
		"""Turns down a running Flink experiment by cancelling all jobs and stopping job and taskmanagers.

		Same as turn_down function but only called when system exits unexpectedly.

		"""
		if not self.turned_down:
			print("Deconstructing Flink experiment")
			self.turn_down()


def rand_mac():
	"""Helper function to generate a random MAC address.

	:return: A random MAC address starting with 2c:4d
	"""
	return "2c:4d:%02x:%02x:%02x:%02x" % (
		random.randint(0, 255),
		random.randint(0, 255),
		random.randint(0, 255),
		random.randint(0, 255)
	)

def start_zookeeper(kafka_path, data_dir, client_port, max_client_cnxns):
	"""Starts a zookeeper instance on localhost.

	:param kafka_path: Path to Kafka folder.
	:param data_dir: Parameter of zookeeper config.
	:param client_port: Parameter of zookeeper config.
	:param max_client_cnxns: Parameter of zookeeper config.

	"""
	zkconfig = open("zkConfig.temp", "w+")
	zkconfig.write("dataDir=" + data_dir + "\n")
	zkconfig.write("clientPort="+str(client_port)+"\n")
	zkconfig.write("maxClientCnxns="+str(max_client_cnxns)+"\n")
	zkconfig.flush()
	zkconfig.close()
	print("Starting zookeeper server")
	os.system(kafka_path + 'bin/zookeeper-server-start.sh zkConfig.temp >zookeeper.log &')


def start_app(app_path, app_arguments):
	"""Use to start a Java application such as a Kafka consumer.

	:param app_path: Path to application jar
	:param app_arguments: Arguments for application

	"""
	print("Starting consumer on host!")
	os.system('java -jar ' + app_path + ' ' + app_arguments + ' &')


def measure_throughput(of, dur, iface='br0'):
	"""Measures the throughput on interface in 1 second intervals.

	All results are in Bps

	:param of: File to write results to (one measurement per second)
	:param dur: Duration in seconds
	:param iface: Network interface to measure on
	:return: A triple of the means measured (TX, RX, Total)

	"""
	file = open(of, 'w')
	tx = 0
	rx = 0
	counter = 0
	tx_total = 0
	rx_total = 0
	file.write("TX RX TOT [Bps]")
	while counter <= dur:
		with open('/sys/class/net/' + iface + '/statistics/tx_bytes', 'r') as f:
			tx_cur = int(f.read())
			if tx > 0:
				tx_speed = tx_cur-tx
				file.write(str(tx_speed) + " ")
				tx_total += tx_speed
			tx = tx_cur
		with open('/sys/class/net/' + iface + '/statistics/rx_bytes', 'r') as f:
			rx_cur = int(f.read())
			if rx > 0:
				rx_speed = rx_cur-rx
				file.write(str(rx_speed) + " " + str(tx_speed + rx_speed) + "\n")
				rx_total += rx_speed
			rx = rx_cur
		counter += 1
		time.sleep(1)
	file.close()
	return (tx_total/dur, rx_total/dur, (tx_total+rx_total)/dur)

def get_pid(grep, args):
	"""Returns the pid of a given process name grep with the specific arguments args
	
	:param grep: name of the process
	:param args: additional grep parameter if more than one process with the given name (grep) exist
	
	"""
	try:
		if args != "":
			qemu_pids = sub.check_output('pgrep ' + grep + ' -a | grep ' + args, shell=True, universal_newlines=True)
		else:
			qemu_pids = sub.check_output('pgrep ' + grep + ' -a', shell=True, universal_newlines=True)
		pids = str(qemu_pids[:-2]).split('\n')
		for pid in pids:
			return int(pid.lstrip().split(' ')[0])
	except Exception as e:
		print("No processes " + args + " running to retrieve pid")
		print(str(e))
		return -1
