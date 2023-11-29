#!/usr/bin/env python3.7
import pika
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
from gpu_client import send2gpuQ
from pika.exceptions import *


logger = logging.getLogger()
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format)
logging.getLogger('pika').setLevel(logging.INFO)


def stage_initer(values):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='stage_gpuQ', durable=True)

    def callback(ch, method, properties, body):
        values.job = body.decode()
        logging.info(f'got it {values.job}')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        begin_main(values)
        logging.info('Done')
        logging.info("Ack'ed")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_message_callback=callback,
                          queue='stage_gpuQ')
    try:
        channel.start_consuming()
    except (StreamLostError, ConnectionResetError) as e:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()


def begin_main(values):
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)
    logging.info(f'Submitting the following to the gpu_queue: {values.job}')
    send2gpuQ(f'{values.job}')


if __name__ == '__main__':
    parser=ArgumentParser(description='Send any command to the GPU queue.', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-d', '--daemon', dest='daemon', action='store_false', help='Run with AMQP')## deamon mode doesn't quite work as expected yet.
    parser.add_argument('-j', '--job', type=str, help='Job to be sent to the GPUQ.', default='nvidia-smi')
    parser.set_defaults(verbose=False)
    parser.set_defaults(daemon=True)
    parser.set_defaults(job='nvidia-smi')
    values = parser.parse_args()

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

    if values.daemon:
        logging.info('Running in daemon mode')
        stage_initer(values)
    else:
        begin_main(values)
