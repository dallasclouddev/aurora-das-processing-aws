import sys
import logging
import psycopg2
import boto3
import json
import random
import calendar
import time
from datetime import datetime
from psycopg2.extras import LogicalReplicationConnection

my_stream_name = 'Foo'
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    my_connection  = psycopg2.connect(
                      "dbname='postgres' host='mypgdb.xxxxxxxxxxxxx.us-east-1.rds.amazonaws.com' user='repluser' password='replpass'" ,
                      connection_factory = LogicalReplicationConnection)
except:
    logger.error("ERROR: Unexpected error: Could not connect to RDS for PostgreSQL instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS for PostgreSQL instance succeeded")

def handler(event, context):
    """
    This function streams content from RDS for PostgreSQL into Kinesis
    """

    cur = my_connection.cursor()
    cur.create_replication_slot('wal2json_test_slot', output_plugin = 'wal2json')
    cur.start_replication(slot_name = 'wal2json_test_slot', options = {'pretty-print' : 1}, decode= True)

    cur.consume_stream(consume)

def consume(msg):
    kinesis_client.put_record(StreamName=my_stream_name, Data=json.dumps(msg.payload), PartitionKey="default")
    print (msg.payload)