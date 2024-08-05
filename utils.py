import mysql.connector
import os
import logging
import json
import requests
import time
import traceback

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


def pipeline_messenger(title, text, notification_type):
    messenger_colours = {
        'pass': '#00c400',
        'fail': '#c40000',
        'notification': '#0000c4'
    }
    if notification_type not in messenger_colours.keys():
        raise Exception('Invalid notification type')

    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": messenger_colours[notification_type],
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)


def connect_preprod():
    """
    connect to mysql database using predetermined os variables,
     set manually in the settings of the machine.
    """
    db = mysql.connector.connect(
        host=os.environ.get('PREPROD_HOST'),
        user=os.environ.get('PREPROD_ADMIN_USER'),
        passwd=os.environ.get('PREPROD_ADMIN_PASS'),
        database=os.environ.get('PREPROD_DATABASE'))
    cursor = db.cursor()
    return cursor, db


def timer(func):
    """a wrapper that tracks the time it takes for a function to process."""

    def timer_wrapper(*args, **kwargs):
        start_time = time.time()
        function_name = func.__name__
        script_name = __file__

        logger.info(f"Executing function '{function_name}' in script '{script_name}'")

        result = func(*args, **kwargs)

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Function: '{function_name}' in script '{script_name}' took {execution_time} seconds")
        return result

    return timer_wrapper


def pipeline_message_wrap(func):
    def pipeline_message_wrapper(*args, **kwargs):
        # define the azure variables here in a try/except, if it fails then we assume it has been run locally
        try:
            azure_pipeline_name = os.environ.get('BUILD_DEFINITIONNAME')
        except Exception:
            azure_pipeline_name = 'localhost'
        function_name = func.__name__
        script_name = os.path.basename(__file__)
        try:
            __mycode = False

            logger.info('starting func')
            start_time = time.time()
            result = func(*args, **kwargs)
            print(func)
            logger.info('sending message')
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(
                f"Function: '{function_name}' in script '{script_name}' of pipeline '{azure_pipeline_name}' took {execution_time} seconds")
            pipeline_messenger(title=f'{function_name} in {script_name} of project {azure_pipeline_name} has passed!',
                               text=str(f'process took {execution_time} seconds'),
                               notification_type='pass')
            print('this is a test')
        except Exception:
            result = None
            pipeline_messenger(
                title=f'{func.__name__} in {__file__} of script {script_name} of pipeline {azure_pipeline_name} has failed',
                text=str(traceback.format_exc()),
                notification_type='fail')
        return result

    return pipeline_message_wrapper
