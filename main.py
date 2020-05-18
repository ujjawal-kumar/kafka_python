import json
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from kafka import KafkaConsumer
from kafka import KafkaProducer, OffsetAndMetadata
from kafka.structs import TopicPartition

app = dash.Dash()

producer = KafkaProducer(bootstrap_servers='localhost:9092')

add_consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="1",enable_auto_commit='True')
add_consumer.subscribe(['add_res'])

sub_consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="1",enable_auto_commit='True')
sub_consumer.subscribe(['sub_res'])

mul_consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="1",enable_auto_commit='True')
mul_consumer.subscribe(['mul_res'])


app.layout = html.Div(children=[
    html.H1(children='Hello Kafka', style={
        'textAlign': 'center',
    }),
    html.Hr(),
    dcc.Input(id='input-num1',
              placeholder='Enter Number here.',
              type='text',
              value=''),
    dcc.Input(id='input-num2',
              placeholder='Enter Number here.',
              type='text',
              value=''),
    html.Br(),
    html.Br(),
    html.Hr(),
    html.Button('Addition', id='add'),
    html.Button('Substraction', id='sub'),
    html.Button('Multiplication', id='mul'),
    html.Div(id='container')
])


@app.callback(Output('container', 'children'),
              [Input('add', 'n_clicks'),
               Input('sub', 'n_clicks'),
               Input('mul', 'n_clicks')],[
    State('input-num1', 'value'),
    State('input-num2', 'value')
])
def display(btn1, btn2, btn3,value1,valu2):
    ctx = dash.callback_context
    result = None
    ctx_msg = json.dumps({
        'states': ctx.states,
        'triggered': ctx.triggered,
        'inputs': ctx.inputs
    }, indent=2)

    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if ctx.states["input-num1.value"].isdigit() and ctx.states["input-num2.value"].isdigit():
            num1 = ctx.states["input-num1.value"]
            num2 = ctx.states["input-num2.value"]
            producer_input_str = num1+","+num2
            producer_input = bytes(producer_input_str, "UTF-8")

            if button_id == 'add':
                producer.send('add_nums', producer_input)
                producer.flush()
                print('++++++++++++++++++++++++++++++++++   add-res  +++++++++++++++++++++++++++++++++++++++++++')
                for message in add_consumer:
                    tp = TopicPartition(message.topic, message.partition)
                    offsets = {tp: OffsetAndMetadata(message.offset, None)}
                    add_consumer.seek_to_end(tp)
                    last_offset = add_consumer.position(tp)
                    add_consumer.commit(offsets=offsets)
                    print(message.value)
                    sum = message.value.decode()
                    result ='The input value was "{}" and "{}" the sum is {}'.format(
                    num1, num2, sum)
                    return html.Div([html.H1(result)])

            if button_id == 'sub':
                producer.send('sub_nums', producer_input)
                producer.flush()
                print('++++++++++++++++++++++++++++++++++   sub-res  +++++++++++++++++++++++++++++++++++++++++++')
                for message in sub_consumer:
                    tp = TopicPartition(message.topic, message.partition)
                    offsets = {tp: OffsetAndMetadata(message.offset, None)}
                    sub_consumer.seek_to_end(tp)
                    last_offset = sub_consumer.position(tp)
                    sub_consumer.commit(offsets=offsets)
                    print(message.value)
                    diff = message.value.decode()
                    result ='The input value was "{}" and "{}" the diff is {}'.format(
                    num1, num2, diff)
                    return html.Div([html.H1(result)])

            if button_id == 'mul':
                producer.send('mul_nums', producer_input)
                producer.flush()
                print('++++++++++++++++++++++++++++++++++   mul-res  +++++++++++++++++++++++++++++++++++++++++++')
                for message in mul_consumer:
                    tp = TopicPartition(message.topic, message.partition)
                    offsets = {tp: OffsetAndMetadata(message.offset, None)}
                    mul_consumer.seek_to_end(tp)
                    last_offset = mul_consumer.position(tp)
                    mul_consumer.commit(offsets=offsets)
                    print(message.value)
                    mul = message.value.decode()
                    result ='The input value was "{}" and "{}" the mul is {}'.format(
                    num1, num2, mul)
                    return html.Div([html.H1(result)])

    return html.Div([html.H1(result)])


if __name__ == '__main__':
    app.run_server(debug=True)