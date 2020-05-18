from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import KafkaProducer, OffsetAndMetadata
from kafka.structs import TopicPartition

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='latest')
topic_partition = TopicPartition('mul_nums', 0)
consumer.assign([topic_partition])
consumer.seek_to_end(topic_partition)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for message in consumer:
    message = message.value.decode()
    number_list = message.split(",")
    number1 = int(number_list[0])
    number2 = int(number_list[1])
    multiplication_result = number1 * number2
    producer_input =str(multiplication_result)
    producer_input= producer_input.encode()
    print(producer_input)
    producer.send('mul_res', producer_input)
    #producer.flush()
