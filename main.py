import paho.mqtt.client as mqttClient
import time


def on_connect(client, userdata, flags, rc):
    if rc == 0:

        print("Connected to broker")

        global Connected  # Use global variable
        Connected = True  # Signal connection

    else:

        print("Connection failed")


def on_message(client, userdata, message):
    print("Message received:", message.payload.decode('UTF-8'))


Connected = False  # global variable for the state of the connection

broker = 'test.mosquitto.org'
port = 1883
topic_prefix = "service/weather_logger"
username = 'hackathon'
password = 'Autumn2021'


client = mqttClient.Client("Python")  # create new instance
# client.username_pw_set(username, password=password)  # set username and password
client.on_connect = on_connect  # attach function to callback
client.on_message = on_message  # attach function to callback

client.connect(broker, port=port)  # connect to broker

client.loop_start()  # start the loop

while Connected != True:  # Wait for connection
    time.sleep(0.1)

client.subscribe(topic_prefix)

try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("exiting")
    client.disconnect()
    client.loop_stop()