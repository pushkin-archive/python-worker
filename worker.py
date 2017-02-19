#!/usr/bin/env python
import time
import os
import pika
import json
import atexit
from classes.RPCClient import RPCClient


client = RPCClient()

rabbitlink =  os.environ['AMPQ_ADDRESS']


parameters = pika.URLParameters(rabbitlink)


# CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['AMPQ_ADDRESS']))
connection = pika.BlockingConnection(parameters)
print "created connection"

channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)


channel.queue_declare(queue='db_write', durable=True)

def callback(ch, method, properties, body):
    # the key part of this code here is that the python worker can do anything it needs 
    # in this scope, we have access to the user id, the question id, and the users choice
    # but it makes sure that it passes the correlation_id(if any) to the db worker, as well as the reply_to
    # this ensures that the data can be returned on the original channel and the api can respond.
    print " [x] Received %r" % body
    j = json.loads(body)
    rpcmethod = j['method']
    payload = j['payload']
    print " METHOD %r" % rpcmethod
    print " PAYLOAD %r" % payload
    if rpcmethod == 'getQuestion':
        if 'questionId' in payload:
            question_id = payload['questionId']
            print "Finding question with %r" % question_id
            # question = client.call(json.dumps({
            #     'method': 'findQuestion',
            #     'arguments': [question_id]
            # }))
            # print "QUESTION: %r" % question
            # # Pretty self explanatory,
            # # get a user, the question they just answered, and their choice
            # if 'userId' in payload:
            #     user_id = payload['userId']
            #     print "Finding user with %r" % user_id
            #     user = client.call(json.dumps({
            #         'method': 'findUser',
            #         'arguments': [user_id]
            #     }))
            #     print "USER: %r" % user
            # if 'choiceId' in payload:
            #     choice_id = payload['choiceId']
            #     print "Finding choice with %r" % choice_id
            #     choice = client.call(json.dumps({
            #         'method': 'findChoice',
            #         'arguments': [choice_id]
            #     }))
            #     print "CHOICE: %r" % choice

            new_id = question_id + 2
            rpc = {
                'method': 'findQuestion',
                'arguments': [new_id, ['choices']]
            }
            message = json.dumps(rpc)
            routing_key = 'db_rpc_worker'
            correlation_id = properties.correlation_id
            reply_to = properties.reply_to

            # key here is we pass this off to the db worker
            # it replies to the original channel with the original correlation_id

            channel.basic_publish(
                exchange='',
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    correlation_id=correlation_id,
                    reply_to=reply_to
                )
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print "Cannot get next question without at least the questionId"
            ch.basic_ack(delivery_tag=method.delivery_tag)
    if rpcmethod == 'getResults':
        # this method is called when a user needs to get results,
        # feel free to make other rpc calls in here 
        # and any data analysis that is needed
        rpcInput = {
            'method': 'getResults',
            'arguments': [
                payload['userId']
            ]
        }
        user_id = payload['userId']
        routing_key = 'db_rpc_worker'
        results = client.call(json.dumps(rpcInput))
        correlation_id = properties.correlation_id
        reply_to = properties.reply_to
        body = results
        reply_to = properties.reply_to
        print "PUBLISHING TO CHANNEL %r" % body
        channel.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            body=body,
            properties=pika.BasicProperties(
                correlation_id=correlation_id,
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

# channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

def exit_handler():
    if channel.is_open:
        channel.close()
print ' [*] Waiting for messages. To exit press CTRL+C'

atexit.register(exit_handler)
channel.start_consuming()

