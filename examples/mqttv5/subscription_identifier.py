import json
import random
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

class Callbacks:

    def __init__(self):
        self.messages = []
        self.publisheds = []
        self.subscribeds = []
        self.unsubscribeds = []
        self.disconnecteds = []
        self.connecteds = []
        self.conn_failures = []

    def __str__(self):
        return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
            str(self.subscribeds) + \
            str(self.unsubscribeds) + str(self.disconnects)

    def clear(self):
        self.__init__()

    def on_connect(self, client, userdata, flags, reasonCode, properties):
        self.connecteds.append({"userdata": userdata, "flags": flags,
                                "reasonCode": reasonCode, "properties": properties})

    def on_connect_fail(self, client, userdata):
        self.conn_failures.append({"userdata": userdata})

    def wait(self, alist, timeout=2):
        interval = 0.2
        total = 0
        while len(alist) == 0 and total < timeout:
            time.sleep(interval)
            total += interval
        return alist.pop(0)  # if len(alist) > 0 else None

    def wait_connect_fail(self):
        return self.wait(self.conn_failures, timeout=10)

    def wait_connected(self):
        return self.wait(self.connecteds)

    def on_disconnect(self, client, userdata, reasonCode, properties=None):
        self.disconnecteds.append(
            {"reasonCode": reasonCode, "properties": properties})

    def wait_disconnected(self):
        return self.wait(self.disconnecteds)

    def on_message(self, client, userdata, message):
        if hasattr(message.properties, 'SubscriptionIdentifier'):
            for identifier in message.properties.SubscriptionIdentifier:
                match message.properties.SubscriptionIdentifier[0]:
                    case 1:
                        self.__log(message)
                    case 2:
                        self.__check_pm25(message)
        self.messages.append({"userdata": userdata, "message": message})

    def published(self, client, userdata, msgid):
        self.publisheds.append(msgid)

    def wait_published(self):
        return self.wait(self.publisheds)

    def on_subscribe(self, client, userdata, mid, reasonCodes, properties):
        self.subscribeds.append({"mid": mid, "userdata": userdata,
                                 "properties": properties, "reasonCodes": reasonCodes})

    def wait_subscribed(self):
        return self.wait(self.subscribeds)

    def unsubscribed(self, client, userdata, mid, reasonCodes, properties):
        self.unsubscribeds.append({"mid": mid, "userdata": userdata,
                                   "properties": properties, "reasonCodes": reasonCodes})

    def wait_unsubscribed(self):
        return self.wait(self.unsubscribeds)

    def register(self, client):
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_publish = self.published
        client.on_unsubscribe = self.unsubscribed
        client.on_message = self.on_message
        client.on_disconnect = self.on_disconnect
        client.on_connect_fail = self.on_connect_fail
    
    def __log(self, message):
        print("[Received] Topic: %s, Payload: %s" % (message.topic, message.payload))

    def __check_pm25(self, message):
        if json.loads(message.payload)['pm2.5'] > 50:
            print("[Action] Turn on the air purifier...")

def publish(client, topic, payload):
    print("[Publish] Topic: %s, Payload %s" % (topic, payload))
    client.publish(topic, payload)

def waitfor(queue, depth, limit):
    total = 0
    while len(queue) < depth and total < limit:
        interval = .5
        total += interval
        time.sleep(interval)

callback = Callbacks()

client_id = "mqtt-feature-demos-{id}".format(id = random.randint(0, 1000))
client = mqtt.Client(client_id.encode("utf-8"), protocol = mqtt.MQTTv5)
callback.register(client)

client.connect(host = "broker.emqx.io", port = 1883, clean_start = True)
client.loop_start()
response = callback.wait_connected()

sub_properties = Properties(PacketTypes.SUBSCRIBE)
sub_properties.SubscriptionIdentifier = 1
client.subscribe(client_id + "/home/+", qos = 2, properties = sub_properties)
response = callback.wait_subscribed()

sub_properties.SubscriptionIdentifier = 2
client.subscribe(client_id + "/home/PM2_5", qos = 2, properties = sub_properties)
response = callback.wait_subscribed()

publish(client, client_id + "/home/PM2_5", json.dumps({'pm2.5': 60}))
waitfor(callback.messages, 2, 2)

publish(client, client_id + "/home/temperature", json.dumps({'temperature': 27.3}))
waitfor(callback.messages, 3, 2)

client.disconnect()
callback.wait_disconnected()
client.loop_stop()
