from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import KafkaProducer, OffsetAndMetadata
from kafka.structs import TopicPartition

#consumer = KafkaConsumer('add_nums', bootstrap_servers=["localhost:9092"])
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='latest',group_id="1",enable_auto_commit='True')
consumer.subscribe(['add_nums'])
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for message in consumer:
    tp = TopicPartition(message.topic, message.partition)
    offsets = {tp: OffsetAndMetadata(message.offset, None)}
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)
    consumer.commit(offsets=offsets)
    message = message.value.decode()
    number_list = message.split(",")
    number1 = int(number_list[0])
    number2 = int(number_list[1])
    addition_result = number1 + number2
    producer_input =str(addition_result) 
    producer_input= producer_input.encode()
    print(producer_input)
    producer.send('add_res', producer_input)
    producer.flush()
