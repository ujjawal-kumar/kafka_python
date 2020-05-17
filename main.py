import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from kafka import KafkaConsumer
from kafka import KafkaProducer

app = dash.Dash()

producer = KafkaProducer(bootstrap_servers='localhost:9092')

add_consumer = KafkaConsumer('add_res', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="test-consumer-group")#,auto_offset_reset='earliest',group_id="test-consumer-group",enable_auto_commit=True)

sub_consumer = KafkaConsumer('sub_res', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="test-consumer-group")#,auto_offset_reset='earliest',group_id="test-consumer-group",enable_auto_commit=True)

mul_consumer = KafkaConsumer('mul_res', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="test-consumer-group")#,auto_offset_reset='earliest',group_id="test-consumer-group",enable_auto_commit=True)

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
    html.Button('Addition', id='add',n_clicks=0),
    html.Button('Substraction', id='sub',n_clicks=0),
    html.Button('Multiplication', id='mul',n_clicks=0),
    html.Br(),
    html.Br(),
    html.Hr(),
    html.Div(id='display-result'),
    html.Div(id='display-result1'),
    html.Div(id='display-result2')
])


@app.callback(Output('display-result', 'children'), [Input('add', 'n_clicks')],[
    State('input-num1', 'value'),
    State('input-num2', 'value')
])
def update_output(n_clicks, value1,value2):
    if  n_clicks :
        if  value1.isdigit() and value2.isdigit():
            producer_input_str = value1+","+value2
            in_msg = value1+"+"+value2
            producer_input = bytes(producer_input_str, "UTF-8")
            producer.send('add_nums', producer_input)
            print('++++++++++++++++++++++++++++++++++   add-res  +++++++++++++++++++++++++++++++++++++++++++')
            for message in add_consumer:
                print(message.value)
                sum = message.value.decode()
                msg,sum = sum.split('::')
                if in_msg == msg:
                    return 'The input value was "{}" and "{}" the sum is {}'.format(
                    value1, value2, sum)
        else:
            return 'The input is not valid'
    else:
        return None



@app.callback(Output('display-result1', 'children'),[Input('sub', 'n_clicks')],[
    State('input-num1', 'value'),
    State('input-num2', 'value')
])
def update_output(n_clicks, value1, value2):
    if n_clicks:
        if value1.isdigit() and value2.isdigit():
            producer_input_str = value1+","+value2
            in_msg = value1+"-"+value2
            producer_input = bytes(producer_input_str, "UTF-8")
            producer.send('sub_nums', producer_input)
            print("---------------------------------  sub-test -------------------------------------")
            for message in sub_consumer:
                print(message.value)
                sub = message.value.decode()
                msg,sub = sub.split('::')
                if in_msg == msg:
                    return 'The input value was "{}" and "{}" the substraction is {}'.format(
                    value1, value2, sub)
        else:
            return 'The input is not valid'
    else:
        return None


@app.callback(Output('display-result2', 'children'),[Input('mul', 'n_clicks')],[
    State('input-num1', 'value'),
    State('input-num2', 'value')
])
def update_output(n_clicks, value1, value2):
    if n_clicks:
        if value1.isdigit() and value2.isdigit():
            producer_input_str = value1+","+value2
            in_msg = value1+"*"+value2
            producer_input = bytes(producer_input_str, "UTF-8")
            producer.send('mul_nums', producer_input)
            print("**********************  mul-test  ************************************")
            for message in mul_consumer:
                print(message.value)
                mul = message.value.decode()
                msg,mul = mul.split('::')
                if in_msg == msg:
                    return 'The input value was "{}" and "{}" the multiplication is {}'.format(
                    value1, value2, mul)
        else:
            return 'The input is not valid'
    else:
        return None


if __name__ == '__main__':
    app.run_server(debug=True)