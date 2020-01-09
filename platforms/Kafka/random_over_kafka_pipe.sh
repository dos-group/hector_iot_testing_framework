#!/bin/bash
function control_c {
    echo -en "\n## Caught SIGINT; Clean up and Exit\n" >> log_script.txt
    bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic 'streams-.*' >> log_script.txt
    kill $APP >> log_script.txt
    echo $APP " (app) killed" >> log_script.txt
    kill $CONNECTOR >> log_script.txt
    echo $CONNECTOR " (connector) killed" >> log_script.txt
    kill $PRODUCER >> log_script.txt
    echo $PRODUCER " (random) killed" >> log_script.txt
    kill $CONSUMER >> log_script.txt
    echo $CONSUMER " (consumer) killed" >> log_script.txt
    bin/kafka-server-stop.sh >> log_script.txt
    kill $BROKER >> log_script.txt
    echo $BROKER " (broker) killed" >> log_script.txt
    bin/zookeeper-server-stop.sh
    kill $ZOOKEEPER >> log_script.txt
    echo $ZOOKEEPER " (zookeeper) killed" >> log_script.txt
    rm test.txt >> log_script.txt
    sleep 5
    kill -9 $APP >> log_script.txt
    echo "Everyone is DEAD. Bye." >> log_script.txt
    exit $?
}

trap control_c SIGINT
rm log_script.txt
touch log_script.txt
bin/zookeeper-server-start.sh config/zookeeper.properties & ZOOKEEPER=$!
sleep 3
echo "ZOOKEEPER STARTED\n" >> log_script.txt
bin/kafka-server-start.sh config/server.properties & BROKER=$!
sleep 3
echo "SERVER STARTED\n" >> log_script.txt
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-plaintext-input >> log_script.txt
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-pipe-output >> log_script.txt
echo "TOPICS CREATED" >> log_script.txt
cd StreamsApp/streams.examples
mvn clean package
mvn exec:java -Dexec.mainClass=myapps.Pipe & APP=$!
sleep 2
cd ../..
rm test.txt
touch test.txt
bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties & CONNECTOR=$!
sleep 3
echo "Starting Experiment\n" >> log_script.txt
../random.sh > test.txt & PRODUCER=$!
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streams-pipe-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.value=true & CONSUMER=$!

while :
do
  echo " "
  sleep 1
done
