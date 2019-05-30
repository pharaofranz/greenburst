#!/usr/bin/env python3

from slackclient import SlackClient
import yaml
import logging

def send_msg_2_slack(msg):
    with open("config/conf.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
    TOKEN = data_loaded['slack']['bot_oauth']
    
    sc = SlackClient(TOKEN)
    
    response= sc.api_call(
      "chat.postMessage",
      channel="CB7BGC70E",
      text=msg
    )
    return response

def send_img_2_slack(img):
    with open("config/conf.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
    TOKEN = data_loaded['slack']['bot_oauth']
    
    sc = SlackClient(TOKEN)
    attachments = [{"title": "Candidate", "image_url": img}]
    response=sc.api_call("chat.postMessage", channel='CB7BGC70E', text='Potential Candidate',
                attachments=attachments)
    return response


if __name__ == "__main__":
    logger = logging.getLogger()
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=format)
    respose = send_msg_2_slack("Hello from Python! :tada:")
    logging.info(f'{respose}')
