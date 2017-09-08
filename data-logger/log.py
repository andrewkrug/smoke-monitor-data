import binascii
import boto3
import grovepi
import logging
import os
import time

from datetime import datetime


logger = logging.getLogger('smoke-logger')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
logger.addHandler(ch)

class DynamoDB(object):
    """Primary object containing dynamodb functions."""
    def __init__(self):
        self.data_table_name = 'smoke-monitor'
        self.dynamodb = None

    def connect_dynamodb(self):
        if self.dynamodb is None:
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            table = dynamodb.Table(self.data_table_name)
            self.dynamodb = table

    def find_or_create_by(self, log_dict, data_key):

        current_logs = self.find(data_key)

        # If the alert is duplicate false do not create another instance of it.
        for log_entry in current_logs:
            if log_entry.get('alert_code') == log_dict.get('alert_code'):
                return None
            else:
                continue

        # Else create another alert.
        return self.create(log_dict)

    def create(self, log_dict):
        self.connect_dynamodb()

        log_dict['data_key'] = self._create_id()
        response = self.dynamodb.put_item(
            Item=log_dict
        )

        return response

    def destroy(self, alert_id, user_id):
        self.connect_dynamodb()

        response = self.dynamodb.delete_item(
            Key={
                'data_key': data_key
            }
        )

        return response

    def update(self, data_key, log_dict):
        self.connect_dynamodb()

        log_dict['data_key'] = data_key
        response = self.dynamodb.put_item(
            Item=log_dict
        )

        return response

    def find(self, data_key):
        self.connect_dynamodb()

        response = self.dynamodb.scan(
            FilterExpression=Attr('data_key').eq(data_key)
        )

        return response.get('Items')

    def _create_id(self):
        """
        :return: random alertid
        """
        return binascii.b2a_hex(os.urandom(15))


class Sensor(object):
    def __init__(self, type, pin):
        self.type = type
        self.pin = pin
        self.mode = "INPUT"
        grovepi.pinMode(self.pin, self.mode)

    def get_value(self):
        sensor_value = grovepi.analogRead(self.pin)
        return sensor_value

    def log_value(self, value):
        log_entry = {
            'type': self.type,
            'value': value,
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        d = DynamoDB()
        d.create(log_entry)
        logger.info('Log entry {l} created in DynamoDb'.format(l=log_entry))


if __name__ == "__main__":
    while True:
        try:
            s = Sensor(type='chemical', pin=0)
            s.log_value(value=s.get_value())
            logger.info('Sleeping 5 minutes')
            time.sleep(5 * 60)
        except Exception as e:
            logger.error('There was a problem polling the sensor {e}'.format(e=e))

