import paho.mqtt.client as mqttClient
import time
import sqlite3
import datetime


def export_info(sensor_info):
    conn = sqlite3.connect("taskmanager/db.sqlite3")
    cur = conn.cursor()
    cur.execute(
        f"""INSERT INTO {sensor_info[0]} (value, date, time)
        VALUES (?, ?, ?);
        """,
        sensor_info[1:]
    )
    conn.commit()
    cur.close()
    conn.close()


def on_connect(client, userdata, flags, rc):
    if rc == 0:

        print("Connected to broker")

        global Connected  # Use global variable
        Connected = True  # Signal connection

    else:

        print("Connection failed")


sensors_info = []


def on_message(client, userdata, message):
    print(message.topic+' '+message.payload.decode('UTF-8'))
    now = datetime.datetime.now()
    _, _, topic = message.topic.split('/')
    value = message.payload.decode('UTF-8')
    sensors_info = [topic, value, now.strftime('%d/%m/%Y'), now.strftime('%H:%M:%S')]
    export_info(sensors_info)


Connected = False  # global variable for the state of the connection

broker = 'mqtt0.bast-dev.ru'
port = 1883
topic_prefix = "service/weather_logger/#"
username = 'hackathon'
password = 'Autumn2021'


client = mqttClient.Client("Python")  # create new instance
client.username_pw_set(username, password=password)  # set username and password
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