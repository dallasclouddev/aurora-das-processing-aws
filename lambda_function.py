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
    print(f"REGION_NAME: {REGION_NAME}")
    print(f"RESOURCE_ID: {RESOURCE_ID}")
    print(f"BUCKET_NAME: {BUCKET_NAME}")

    print("\nEvent structure:")
    print(json.dumps(event, indent=2))

    processed_records = []
    for idx, dasRecord in enumerate(event['Records']):
        print(f"\nProcessing record {idx + 1} of {len(event['Records'])}")
        data = base64.b64decode(dasRecord['kinesis']['data'])
        print(f"Decoded data length: {len(data)}")

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

    print(f"\nFinal summary:")
    print(f"Total records processed: {len(processed_records)}")
    return processed_records


def processDASRecord(rec):
    try:
        print("Starting processDASRecord")
        record = json.loads(rec)
        print(f"Record type: {record.get('type')}")

        if record['type'] == 'DatabaseActivityMonitoringRecords':
            print("Processing DatabaseActivityMonitoringRecords")
            dbEvents = record["databaseActivityEvents"]
            dataKey = base64.b64decode(record['key'])
            print("Extracted dbEvents and key")

            try:
                print(f"Attempting KMS decrypt with resource ID: {RESOURCE_ID}")
                data_key_decrypt_result = kms.decrypt(
                    CiphertextBlob=dataKey,
                    EncryptionContext={'aws:rds:dbc-id': RESOURCE_ID}
                )
                print("KMS decrypt successful")

                plaintextEvents = decrypt_decompress(
                    base64.b64decode(dbEvents),
                    data_key_decrypt_result['Plaintext']
                )
                print(f"Events decrypted and decompressed")

                events = json.loads(plaintextEvents)
                event_list = events.get('databaseActivityEventList', [])
                print(f"Found {len(event_list)} events to process")

                retObj = []
                for idx, dbEvent in enumerate(event_list):
                    print(f"\nProcessing event {idx + 1} of {len(event_list)}")
                    print(f"Full event data: {json.dumps(dbEvent, indent=2)}")

                    event_type = dbEvent.get('type')
                    print(f"Event type from data: {event_type}")

                    if event_type == "heartbeat":
                        print("Skipping heartbeat event")
                        continue

                    # Get command and commandText
                    command = dbEvent.get('command')
                    commandText = dbEvent.get('commandText', '')
                    print(f"Command: {command}")
                    print(f"CommandText: {commandText}")


                    # Determine event type based on command text
                    if commandText:
                        upperCommandText = commandText.upper()
                        print(f"Analyzing commandText: {upperCommandText}")

                        if 'DELETE' in upperCommandText:
                            eventType = 'DELETE'
                            print("Found DELETE operation")
                        elif 'INSERT' in upperCommandText:
                            eventType = 'INSERT'
                            print("Found INSERT operation")
                        elif 'UPDATE' in upperCommandText:
                            eventType = 'UPDATE'
                            print("Found UPDATE operation")
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
                    timestamp = datetime.now()
                    s3_key = f"parsed/{eventType}/{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/das-{timestamp.strftime('%Y%m%d-%H%M%S-%f')}.json"
                    print(f"Generated S3 key: {s3_key}")

                    try:
                        print(f"Writing to S3: {BUCKET_NAME}/{s3_key}")
                        response = s3.put_object(
                            Bucket=BUCKET_NAME,
                            Key=s3_key,
                            Body=json.dumps(dbEvent, indent=2, ensure_ascii=False)
                        )
                        print(f"Successfully wrote to S3, response: {response}")
                        retObj.append(dbEvent)
                    except Exception as e:
                        print(f"Error writing to S3: {str(e)}")
                        raise

                print(f"Successfully processed {len(retObj)} events")
                return retObj

            except Exception as e:
                print(f"Error processing events: {str(e)}")
                raise

        else:
            print(f"Skipping non-DAS record of type: {record.get('type')}")
            return []

    except Exception as e:
        print(f"Error in processDASRecord: {str(e)}")
        raise



