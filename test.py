from kafka import KafkaConsumer
from kafka import KafkaProducer

add_producer = KafkaProducer(bootstrap_servers='localhost:9092')
# add_consumer = KafkaConsumer('add_res', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="test-consumer-group",enable_auto_commit=True)
add_consumer = KafkaConsumer('add_res', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="test-consumer-group",enable_auto_commit=True)
while 1:
    num1 = input()
    num2 = input()
    producer_input_str = num1+","+num2
    producer_input = bytes(producer_input_str, "UTF-8")
    add_producer.send('add_nums', producer_input)
    for message in add_consumer:
        #print(message.value)
        sum = message.value.decode()
        msg,sum = sum.split('::')
        print(msg)
        
