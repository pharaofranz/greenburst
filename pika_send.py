#!/usr/bin/env python3

import pika
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging

def send2Q(queue,body):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(exchange='',routing_key=queue,body=body, properties=pika.BasicProperties(delivery_mode = 2))
        logging.info(f'Sent {body}')
        return connection.close()
    except:
        return None

if __name__ == "__main__":
    parser=ArgumentParser(description='Message sender', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-q', '--queue', type=str, help='send message to this queue', required=True)
    parser.add_argument('-m', '--message',type=str, help='send this message', required=True)
    parser.set_defaults(verbose=False)
    values = parser.parse_args()
    logger = logging.getLogger()

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)
        logging.getLogger('pika').setLevel(logging.INFO)
    logging.info(f'message reads {values.message}')
    if values.queue not in ['stage01_queue','stage02_queue','stage03_queue','stage_gpuQ']:
        logging.error(f'{values.queue} not found')
    else:
        send2Q(values.queue,values.message)
