#!/usr/bin/env python
import time
import os
import pika
print "worker"


rabbitlink =  os.environ['AMPQ_ADDRESS']


parameters = pika.URLParameters(rabbitlink)

print rabbitlink
print "PARAMS"
print parameters

# CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['AMPQ_ADDRESS']))
connection = pika.BlockingConnection(parameters)
print "created connection"

channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

writechannel = connection.channel()

writechannel.queue_declare('db_write', durable=True)

def callback(ch, method, properties, body):
    print " [x] Received %r" % body
    count = int(body) + 1
    time.sleep(body.count(b'.'))
    print " [x] Done"
    writechannel.basic_publish(exchange='', routing_key='db_queue', body=str(count), properties=pika.BasicProperties(
        delivery_mode=2
    ))
    ch.basic_ack(delivery_tag=method.delivery_tag)

# channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

print ' [*] Waiting for messages. To exit press CTRL+C'
channel.start_consuming()
