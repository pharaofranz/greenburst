#!/usr/bin/env python3

#from slackclient import SlackClient
from slack import WebClient as SlackClient

import yaml
import logging

def send_msg_2_slack(msg):
    with open("config/conf.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
    TOKEN = data_loaded['slack']['bot_oauth']
    
    client = SlackClient(TOKEN)
    response = client.chat_postMessage(
      channel="CPAK5A4G2",
      text=msg
    )
    return response

def send_img_2_slack(img):
    with open("config/conf.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
    TOKEN = data_loaded['slack']['bot_oauth']
    
    client = SlackClient(TOKEN)
    attachments = [{"title": "Candidate", "image_url": img}]
    response = client.chat_postMessage(channel='CPAK5A4G2', text='Potential Candidate',
                attachments=attachments)
    return response


if __name__ == "__main__":
    logger = logging.getLogger()
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=format)
    respose = send_msg_2_slack("Hello from Python! :tada:")
    logging.info(f'{respose}')
    respose = send_img_2_slack('https://images.sftcdn.net/images/t_app-cover-l,f_auto/p/befbcde0-9b36-11e6-95b9-00163ed833e7/260663710/the-test-fun-for-friends-screenshot.jpg')
