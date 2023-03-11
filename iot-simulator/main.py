import time
import paho.mqtt.client as paho
import ssl
import json

from keys import *

#define callbacks
def on_message(client, userdata, message):
  print("received message =",str(message.payload.decode("utf-8")))

def on_log(client, userdata, level, buf):
  print("log: ",buf)

def on_connect(client, userdata, flags, rc):
  print("publishing ")
  client.publish("pico","test",)


client=paho.Client() 
client.on_message=on_message
client.on_log=on_log
client.on_connect=on_connect
print("connecting to broker")
client.tls_set(ROOT_FILE, certfile=CERTFILE, keyfile=KEYFILE, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
client.tls_insecure_set(True)
client.connect(AWS_IOT_HOST, 8883, 60)

##start loop to process received messages
client.loop_start()
#wait to allow publish and logging and exit
time.sleep(1)

client.publish('pico_test', json.dumps({"message": "Hello COMP680"}), qos=1)