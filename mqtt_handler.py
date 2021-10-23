import paho.mqtt.client as mqttClient
import time
import psycopg2
import datetime
from psycopg2 import Error


def export_info(sensor_info):
    try:
        conn = psycopg2.connect(dbname='postgres', user='postgres', host='localhost', password='admin')
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO sensor_info (air_temp, ground_temp, air_humidity, soil_humidity, illumination, date, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            sensor_info
        )
        conn.commit()
        cur.close()
        conn.close()
        print("Соединение с PostgreSQL закрыто")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def on_connect(client, userdata, flags, rc):
    if rc == 0:

        print("Connected to broker")

        global Connected  # Use global variable
        Connected = True  # Signal connection

    else:

        print("Connection failed")

sensors_info = []


def on_message(client, userdata, message):
    now = datetime.datetime.now()
    inputdata = message.payload.decode('UTF-8')
    sensors_info = [*inputdata.split('\n'), now.strftime('%d/%m/%Y'), now.strftime('%H:%M:%S')]
    for i in range(5):
        _, sensors_info[i] = sensors_info[i].split(':')
        sensors_info[i] = int(sensors_info[i])
    export_info(sensors_info)


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