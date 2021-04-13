#!/usr/bin/env python
#coding=utf-8
import hmac
import base64
from hashlib import sha1
import time
from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from paho.mqtt import client as mqtt
import mysql.connector
import json
import time
import math

#
#   阿里云MQTT服务设置
#
instanceId =''   # 实例 ID，购买后从产品控制台获取
accessKey = ''  #账号AccessKey 从阿里云账号控制台获取
secretKey = ''    #账号secretKey 从阿里云账号控制台获取
groupId = ''    #MQTT GroupID,创建实例后从 MQTT 控制台创建
client_id=groupId+'@@@'+''    #MQTT ClientID，由 GroupID 和后缀组成，需要保证全局唯一
topic = ''    # Topic， 其中第一级父级 Topic 需要从控制台创建
brokerUrl=''   #MQTT 接入点域名，实例初始化之后从控制台获取

#
#   MySql数据库设置
#
dbHost = '127.0.0.1'    #数据库地址
dbName = 'mqtt'     #数据库名称
dbUser = 'root'     #数据库帐户名
dbPass = 'root'     #数据库帐户密码

#监控日志
def on_log(client, userdata, level, buf):
    if level == MQTT_LOG_INFO:
        head = 'INFO'
    elif level == MQTT_LOG_NOTICE:
        head = 'NOTICE'
    elif level == MQTT_LOG_WARNING:
        head = 'WARN'
    elif level == MQTT_LOG_ERR:
        head = 'ERR'
    elif level == MQTT_LOG_DEBUG:
        head = 'DEBUG'
    else:
        head = level
    print('%s: %s' % (head, buf))

#连接后执行此函数
def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe(topic, 0)
    #rc = client.publish(topic, str("卢本伟牛逼"), qos=0)
    time.sleep(0.1)

#如果订阅到消息执行此函数
def on_message(client, userdata, msg):
    saveMsg(msg)

#断开连接
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print('Unexpected disconnection %s' % rc)

def saveMsg(msg):
    db = mysql.connector.connect(
        host=dbHost,
        user=dbUser,
        passwd=dbPass,
        database=dbName
    )
    c = db.cursor()
    print(msg.payload)
    print(str(time.time() / 1000))
    sql = "INSERT INTO `messages`(client_id,topic,msg,updatetime) VALUES (%s,%s,%s,%s)"
    payload = str(msg.payload, encoding='utf-8')
    jsonpayload = json.loads(payload)
    val = (str(client_id), str(msg.topic), payload, int(math.floor(time.time())))
    c.execute(sql, val)
    db.commit()
    print("#"+ msg.topic +"\n消息："+jsonpayload["msg"])

client = mqtt.Client(client_id, protocol=mqtt.MQTTv311, clean_session=True)
client.on_log = on_log
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

## username和 Password 签名模式下的设置方法，参考文档 https://help.aliyun.com/document_detail/48271.html?spm=a2c4g.11186623.6.553.217831c3BSFry7
userName ='Signature'+'|'+accessKey+'|'+instanceId
password = base64.b64encode(hmac.new(secretKey.encode(), client_id.encode(), sha1).digest()).decode()
print(userName) #输出计算的用户名
print(password) #输出计算的密码
client.username_pw_set(userName, password)
# ssl设置，并且port=8883
#client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
client.connect(brokerUrl, 1883, 60)
client.loop_forever()