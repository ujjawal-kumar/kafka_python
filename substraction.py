from kafka import KafkaConsumer
from kafka import KafkaProducer

consumer = KafkaConsumer('sub_nums', bootstrap_servers=["localhost:9092"])

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for message in consumer:
    message = message.value.decode()
    print(message)
    number_list = message.split(",")
    number1 = int(number_list[0])
    number2 = int(number_list[1])
    substraction_result = number1 - number2
    print(substraction_result)
    producer_input =str(number1)+"-"+str(number2)+"::"+str(substraction_result)
    producer_input= producer_input.encode()
    print(producer_input)
    producer.send('sub_res', producer_input)


