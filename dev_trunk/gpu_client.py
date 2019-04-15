#!/usr/bin/env python
import pika
import uuid
import logging
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


class GPURpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='gpu_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=n)
        while self.response is None:
            self.connection.process_data_events()
        return self.response


def send2gpuQ(cmd):
    gpu_rpc_Q = GPURpcClient()
    response = gpu_rpc_Q.call(cmd)
    logging.info('Got the following response')
    logging.info(response)
    return response

if __name__ == "__main__":
    parser=ArgumentParser(description='GPU RPC Server', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.add_argument('-m', '--gpu_cmd',type=str, default='nvidia-smi')
    parser.set_defaults(verbose=False)
    values = parser.parse_args()
    logger = logging.getLogger()

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)
        logging.getLogger('pika').setLevel(logging.INFO)

    send2gpuQ(values.gpu_cmd)
