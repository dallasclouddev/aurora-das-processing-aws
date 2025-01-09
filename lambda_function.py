# This Lambda function reads the Kinesis Firehose records as Input, decrypt the log records using KMS key, unzip the records and then categories the event type into S3 folder structure.
import os
import json
import boto3
import base64
import zlib
import aws_encryption_sdk
from aws_encryption_sdk import CommitmentPolicy
from aws_encryption_sdk.internal.crypto import WrappingKey
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
from aws_encryption_sdk.identifiers import WrappingAlgorithm, EncryptionKeyType
from datetime import datetime
import pytz
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
import sys

REGION_NAME = os.environ['region_name']  # 'us-east-1'
RESOURCE_ID = os.environ['resource_id']  # 'cluster-2VRZBI263EBXMYD3BQUFSIQ554'
BUCKET_NAME = os.environ['bucket_name']  # 'dastestbucket'

enc_client = aws_encryption_sdk.EncryptionSDKClient(commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_ALLOW_DECRYPT)
kms = boto3.client('kms', region_name=REGION_NAME)
s3 = boto3.client('s3')
todays_date = datetime.now()


class MyRawMasterKeyProvider(RawMasterKeyProvider):
    provider_id = "BC"

    def __new__(cls, *args, **kwargs):
        obj = super(RawMasterKeyProvider, cls).__new__(cls)
        return obj

    def __init__(self, plain_key):
        RawMasterKeyProvider.__init__(self)
        self.wrapping_key = WrappingKey(wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                                        wrapping_key=plain_key, wrapping_key_type=EncryptionKeyType.SYMMETRIC)

    def _get_raw_key(self, key_id):
        return self.wrapping_key


def decrypt_payload(payload, data_key):
    my_key_provider = MyRawMasterKeyProvider(data_key)
    my_key_provider.add_master_key("DataKey")
    # Decrypt the records using the master key.
    decrypted_plaintext, header = enc_client.decrypt(
        source=payload,
        materials_manager=aws_encryption_sdk.materials_managers.default.DefaultCryptoMaterialsManager(
            master_key_provider=my_key_provider))
    return decrypted_plaintext


def decrypt_decompress(payload, key):
    decrypted = decrypt_payload(payload, key)
    # Decompress the records using zlib library.
    decrypted = zlib.decompress(decrypted, zlib.MAX_WBITS + 16)
    return decrypted


# Lambda Handler entry point
def lambda_handler(event, context):
    print(f"Environment variables:")
    logger.debug(f"REGION_NAME: {REGION_NAME}")
    print(f"RESOURCE_ID: {RESOURCE_ID}")
    print(f"BUCKET_NAME: {BUCKET_NAME}")

    logger.debug("\nEvent structure:")
    logger.debug(json.dumps(event, indent=2))

    processed_records = []
    for idx, dasRecord in enumerate(event['Records']):
        logger.info(f"\nProcessing record {idx + 1} of {len(event['Records'])}")
        data = base64.b64decode(dasRecord['kinesis']['data'])
        logger.info(f"Decoded data length: {len(data)}")

        try:
            val = processDASRecord(data)
            if val:
                processed_records.extend(val)
                print(f"Successfully processed record {idx + 1}")
            else:
                print(f"No data returned for record {idx + 1}")
        except Exception as e:
            print(f"Error processing record {idx + 1}: {str(e)}")
            raise e

    logger.debug(f"\nFinal summary:")
    print(f"Total records processed: {len(processed_records)}")
    return processed_records


def calculate_duration(event):
    try:
        # Convert string timestamps to datetime objects
        start_time = datetime.strptime(event['startTime'], '%Y-%m-%d %H:%M:%S.%f%z')
        log_time = datetime.strptime(event['logTime'], '%Y-%m-%d %H:%M:%S.%f%z')

        # Calculate duration directly in seconds
        duration_seconds = (log_time - start_time).total_seconds()
        logger.info(f"Query duration: {duration_seconds:.3f} seconds")
        # logger.info(f"Query: {event['commandText']}")
        return duration_seconds * 1000000  # Return microseconds for compatibility

    except Exception as e:
        logger.error(f"Error calculating duration: {str(e)}")
        return None


def processDASRecord(rec):
    try:
        logger.info("Starting processDASRecord")
        record = json.loads(rec)
        logger.info(f"Record type: {record.get('type')}")

        if record['type'] == 'DatabaseActivityMonitoringRecords':
            logger.info("Processing DatabaseActivityMonitoringRecords")
            dbEvents = record["databaseActivityEvents"]
            dataKey = base64.b64decode(record['key'])
            logger.info("Extracted dbEvents and key")

            try:
                logger.debug(f"Attempting KMS decrypt with resource ID: {RESOURCE_ID}")
                data_key_decrypt_result = kms.decrypt(
                    CiphertextBlob=dataKey,
                    EncryptionContext={'aws:rds:dbc-id': RESOURCE_ID}
                )
                logger.debug("KMS decrypt successful")

                plaintextEvents = decrypt_decompress(
                    base64.b64decode(dbEvents),
                    data_key_decrypt_result['Plaintext']
                )
                logger.debug(f"Events decrypted and decompressed")

                events = json.loads(plaintextEvents)
                event_list = events.get('databaseActivityEventList', [])
                logger.info(f"Found {len(event_list)} events to process")

                retObj = []
                for idx, dbEvent in enumerate(event_list):
                    logger.info(f"\nProcessing event {idx + 1} of {len(event_list)}")
                    event_type = dbEvent.get('type')
                    if event_type == "heartbeat":
                        print("Skipping heartbeat event")
                        continue
                    logger.info(f"Full event data: {json.dumps(dbEvent, indent=2)}")
                    logger.info(f"Event type: {event_type}")

                    # Get command and commandText
                    command = dbEvent.get('command')
                    commandText = dbEvent.get('commandText', '')
                    logger.info(f"Command: {command}")
                    logger.info(f"CommandText: {commandText}")
                    calculate_duration(dbEvent)
                    sys.stdout.flush()

                    # Determine event type based on command text
                    if commandText:
                        upperCommandText = commandText.upper()
                        logger.debug(f"Analyzing commandText: {upperCommandText}")

                        if 'DELETE' in upperCommandText:
                            eventType = 'DELETE'
                            print("Found DELETE operation")
                        elif 'INSERT' in upperCommandText:
                            eventType = 'INSERT'
                            print("Found INSERT operation")
                        elif 'UPDATE' in upperCommandText:
                            eventType = 'UPDATE'
                            print("Found UPDATE operation")
                            # where_clause = commandText.split('WHERE')[1]
                            # logger.info(f"Records modified matching: {where_clause}")
                        elif 'SELECT' in upperCommandText:
                            eventType = 'SELECT'
                            print("Found SELECT operation")
                        else:
                            eventType = 'OTHER'
                            print(f"No specific operation found, defaulting to OTHER")
                    else:
                        eventType = 'OTHER'
                        print("No command text found, defaulting to OTHER")

                    print(f"Final eventType: {eventType}")

                    # Create S3 key
                    central = pytz.timezone('America/Chicago')
                    timestamp = datetime.now(pytz.UTC).astimezone(central)
                    s3_key = f"das/{eventType}/{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/das-{timestamp.strftime('%Y%m%d-%H')}.json"
                    logger.debug(f"Generated S3 key: {s3_key}")

                    try:
                        logger.info(f"Writing to S3: {BUCKET_NAME}/{s3_key}")
                        response = s3.put_object(
                            Bucket=BUCKET_NAME,
                            Key=s3_key,
                            Body=json.dumps(dbEvent, indent=2, ensure_ascii=False)
                        )
                        logger.debug(f"Successfully wrote to S3, response: {response}")
                        sys.stdout.flush()
                        retObj.append(dbEvent)
                    except Exception as e:
                        logger.error(f"Error writing to S3: {str(e)}")
                        raise

                logger.info(f"Successfully processed {len(retObj)} events")
                return retObj

            except Exception as e:
                logger.error(f"Error processing events: {str(e)}")
                raise

        else:
            logger.info(f"Skipping non-DAS record of type: {record.get('type')}")
            return []

    except Exception as e:
        logger.error(f"Error in processDASRecord: {str(e)}")
        raise
