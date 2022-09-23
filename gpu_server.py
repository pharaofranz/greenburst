#!/usr/bin/env python3.6
import pika
import logging
import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import threading


def run_cmd(gpu_cmd, response):
    response = subprocess.run(gpu_cmd.split(), stdout=subprocess.PIPE).stdout.decode()
    logging.info(response)

def on_request(ch, method, props, body):
    gpu_cmd=body.decode()
    logging.info(f'got it {gpu_cmd}')
    response=None
    #run_cmd(gpu_cmd, response)
    thread = threading.Thread(target=run_cmd, args=(gpu_cmd, response))
    thread.start()
    while thread.is_alive():
        ch._connection.sleep(5.0)
    thread.join()
    logging.info('Done')

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    #while response is None:
    #    connection.process_data_events()
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    parser=ArgumentParser(description='GPU RPC Server', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.set_defaults(verbose=False)
    values = parser.parse_args()
    logger = logging.getLogger()

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)
        logging.getLogger('pika').setLevel(logging.INFO)

    connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()
    channel.queue_declare(queue='gpu_queue')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='gpu_queue', on_message_callback=on_request)
    logging.info('Awaiting jobs in the GPU queue')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Got keyboard interrupt Stopping consuming')
        channel.stop_consuming()
