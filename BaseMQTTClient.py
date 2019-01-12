# coding=utf-8
# author huyidong
# time 2018.1.11
# MQTT客户端


import configparser
import logging
import os
import paho.mqtt.client as mqtt
import threading
import time
import queue

logging.basicConfig(format="%(asctime)s-%(name)s-%(levelname)s-%(message)s", level=logging.DEBUG)
lg = logging.getLogger('mqtt')


class MQTTClient:

    def __init__(self):
        self._client_name = 'mqtt_demo'
        self._target_ip = '127.0.0.1'
        self._target_port = 61613
        self._username = None
        self._password = None
        self._subscribe_topic = None
        self._queue_len = 50
        self._keepalive = 60
        self._work = False
        self._mqtt_client = mqtt.Client(self._client_name)
        self._mutex = threading.Lock()
        self._queue = queue.Queue(maxsize=50)  # 队列长度为50
        self._topic_filter = {'default': self._queue}
        self._encode = 'utf-8'

    def load_config(self):
        """
        尝试读取配置文件，更新必要参数和非必要参数
        :return: 必要的参数，如mqtt服务区的的IP、用户名和密码，读取成功后返回True，必要参数不全返回False
        """
        cf = configparser.ConfigParser()
        if not cf.read('mqtt.cfg'):
            lg.error("读取配置文件失败，读取位置:" + str(os.getcwd()) + "/mqtt.cfg.")
        else:
            try:
                self._target_ip = cf['mqtt']['target_ip']
                self._username = cf['mqtt']['username']
                self._password = cf['mqtt']['password']
                self._work = True
                self._target_port = int(cf['mqtt']['target_port'])
                self._client_name = cf['mqtt']['client_prefix'] + cf['local']['host']
                self._queue_len = int(cf['mqtt']['queue_len'])
                self._keepalive = int(cf['mqtt']['keepalive'])
                self._subscribe_topic = cf['mqtt']['default_subscribe_topic']
                self._queue.maxsize = int(cf['mqtt']['queue_len'])
                self._encode = cf['mqtt']['encode']
            except (KeyError, ValueError):
                lg.exception("配置文件出错！")
        return self._work

    def connect(self, username=None, password=None, client_name=None, target_ip=None, target_port=None):

        self._username = self._username if username is None else username
        self._password = self._password if password is None else password
        self._client_name = self._client_name if client_name is None else client_name
        self._target_ip = self._target_ip if target_ip is None else target_ip
        self._target_port = self._target_port if target_port is None else target_port
        self._mqtt_client = mqtt.Client(self._client_name)
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_publish = self._on_publish
        self._mqtt_client.on_subscribe = self._on_subscribe
        self._mqtt_client.on_message = self._on_message
        self._mqtt_client.username_pw_set(self._username, self._password)
        self._mqtt_client.connect(self._target_ip, self._target_port, self._keepalive)
        self._mqtt_client.loop_start()
        lg.info("\n|>>>>>>>>>>>>>>>尝试连接到MQTT服务器<<<<<<<<<<<<<<<<<<|\n|\n"
                "|MQTT服务器IP:\t" + self._target_ip + "\t端口：" + str(self._target_port) + "\n"
                                                                                       "|本地客户端全称：\t" + self._client_name + "\n"
                                                                                                                           "|用户名：\t" + self._username + "\t密码：" + self._password + "\n"
                                                                                                                                                                                   "|已订阅Topic：\t" + str(
            self._subscribe_topic) + "\n"
                                     "|消息队列长度：\t" + str(self._queue_len) + "\t心跳时长：\t" + str(self._keepalive) + "s\n"
                                                                                                                "|\n|>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<|\n")
        # 等待1秒,避免订阅方法在连接成功之前调用
        time.sleep(1)

    def publish(self, topic, payload=None, qos=0, retain=False):
        self._mqtt_client.publish(topic, payload, qos, retain)

    def subscribe(self, topic, qos=0):
        self._mqtt_client.subscribe(topic, qos)

    def unsubscribe(self, topic):
        self._mqtt_client.unsubscribe(topic)

    def _on_connect(self, client, userdata, flags, rc):
        rc_table = {
            0: "连接成功",
            1: "连接失败：协议版本错误",
            2: "连接失败：客户端标识非法",
            3: "连接失败：服务器不存在",
            4: "连接失败：账户或密码错误",
            5: "连接失败：未授权",
            6: "连接失败：正在使用"
        }
        try:
            if rc == 0:
                self._mqtt_client.subscribe(self._subscribe_topic)
            lg.info(rc_table[rc])
        except KeyError:
            lg.info("连接失败：正在使用")

    def _on_publish(self, client, userdata, mid):
        lg.info("Publish，Trace：" + str(mid) + ".")

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        lg.info("Subscribe，Trace：" + str(mid) + ".")

    def _on_message(self, client, userdata, message):
        """
        :param client:      the client instance for this callback
        :param userdata:    the private user data as set in Client() or userdata_set()
        :param message:     an instance of MQTTMessage.
                            This is a class with members topic, payload, qos, retain.
        """
        payload = message.payload.decode(self._encode)
        lg.info("Get Message:Topic=" + message.topic + ",payload=" + payload + ",qos=" + str(
            message.qos) + ",retain:" + str(message.retain))
        try:
            if message.topic in self._topic_filter:
                self._topic_filter[message.topic].put(payload, block=True, timeout=1)
            else:
                self._queue.put(payload, block=True, timeout=1)
        except queue.Full:
            if message.topic in self._topic_filter:
                lg.info("MQTT消息队列(" + message.topic + ")已满，等待...")
                self._topic_filter[message.topic].get_nowait()
                self._topic_filter[message.topic].put_nowait(payload)
            else:
                lg.info("MQTT消息队列(default)已满，等待...")
                self._queue.get_nowait()
                self._queue.put_nowait(payload)

    def ask_answer(self, pub_topic, message, sub_topic, count=1, timeout=1, qos=2):
        """
        发送MQTT请求，接收单个或多个回复
        :param pub_topic:   发布请求的Topic
        :param message:     发布请求的内容
        :param sub_topic:   接受回复的Topic
        :param count:       最小接受数，满足最小接受数则返回回复结果，默认1
        :param timeout:     在为满足最小接受数时等待的最长时间(单位：s)，默认1000
        :param qos:         服务质量，默认2
        :return: 结果链表
        """
        result = []
        self._topic_filter[sub_topic] = queue.Queue(self._queue_len)
        self.subscribe(sub_topic, qos)
        self.publish(pub_topic, message, qos)
        get = try_times = 0
        sleep_time = 10
        max_try = timeout * 999 / sleep_time
        while get < count and try_times < max_try:
            try_times += 1
            try:
                msg = self._topic_filter[sub_topic].get(block=True, timeout=sleep_time / 1000)
                result.append(str(msg))

                get += 1
            except queue.Empty:
                continue
        return result
