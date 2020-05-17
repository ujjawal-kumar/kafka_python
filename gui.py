import json

import dash
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_core_components as dcc

from kafka import KafkaConsumer
from kafka import KafkaProducer


app = dash.Dash(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

consumer = KafkaConsumer('ops_res', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id="test-consumer-group")

app.layout = html.Div([
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
        #print(button_id)
        #print(ctx.states)
        if button_id == 'add':
            if ctx.states["input-num1.value"].isdigit() and ctx.states["input-num2.value"].isdigit():
                num1 = ctx.states["input-num1.value"]
                num2 = ctx.states["input-num2.value"]
                producer_input_str = num1+"+"+num2
                in_msg = num1+"+"+num2
                producer_input = bytes(producer_input_str, "UTF-8")
                producer.send('ops_msg', producer_input)
                print(num1,num2)
                print('++++++++++++++++++++++++++++++++++   add-res  +++++++++++++++++++++++++++++++++++++++++++')
                for message in consumer:
                    print(message.value)
                    sum = message.value.decode()
                    msg,sum = sum.split('::')
                    if in_msg == msg:
                        result ='The input value was "{}" and "{}" the sum is {}'.format(
                        num1, num2, sum)
                        return html.Div([html.H1(result)])
            pass
        if button_id == 'sub':
            if ctx.states["input-num1.value"].isdigit() and ctx.states["input-num2.value"].isdigit():
                num1 = ctx.states["input-num1.value"]
                num2 = ctx.states["input-num2.value"]
                print(num1,num2)
                producer_input_str = num1+"-"+num2
                in_msg = num1+"-"+num2
                producer_input = bytes(producer_input_str, "UTF-8")
                producer.send('ops_msg', producer_input)
                print(num1,num2)
                print('++++++++++++++++++++++++++++++++++   sub-res  +++++++++++++++++++++++++++++++++++++++++++')
                for message in consumer:
                    print(message.value)
                    sub = message.value.decode()
                    msg,sub = sub.split('::')
                    if in_msg == msg:
                        result ='The input value was "{}" and "{}" the substraction is {}'.format(
                        num1, num2, sub)
                        return html.Div([html.H1(result)])
            pass
        if button_id == 'mul':
            if ctx.states["input-num1.value"].isdigit() and ctx.states["input-num2.value"].isdigit():
                num1 = ctx.states["input-num1.value"]
                num2 = ctx.states["input-num2.value"]
                print(num1,num2)
                producer_input_str = num1+"*"+num2
                in_msg = num1+"*"+num2
                producer_input = bytes(producer_input_str, "UTF-8")
                producer.send('ops_msg', producer_input)
                print(num1,num2)
                print('++++++++++++++++++++++++++++++++++   mul-res  +++++++++++++++++++++++++++++++++++++++++++')
                for message in consumer:
                    print(message.value)
                    mul = message.value.decode()
                    msg,mul = mul.split('::')
                    if in_msg == msg:
                        result ='The input value was "{}" and "{}" the multiplication is {}'.format(
                        num1, num2, mul)
                        return html.Div([html.H1(result)])
            pass

    return html.Div([html.H1(result)])

    


    


if __name__ == '__main__':
    app.run_server(debug=True)