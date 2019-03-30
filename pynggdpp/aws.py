from datetime import datetime
import os
import json
from io import BytesIO
from urllib.parse import urlsplit
from botocore.exceptions import ClientError

import boto3

import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Connect:
    def __init__(self):
        self.type = "client"

    def aws_client(self, service, type="client"):
        if type == "client":
            return boto3.client(service.lower(), endpoint_url=os.environ[f"AWS_HOST_{service}"])
        elif type == "resource":
            return boto3.resource(service.lower(), endpoint_url=os.environ[f"AWS_HOST_{service}"])

    def elastic_client(self):
        return Elasticsearch(hosts=[os.environ["AWS_HOST_Elasticsearch"]])


class Search:
    def __init__(self):
        self.aws = Connect()
        self.es = self.aws.elastic_client()

    def create_es_index(self, index_name, doc_type="ndc_collection_mapping"):
        responses = list()
        body = self.ndc_index_mapping(doc_type=doc_type)
        if self.es.indices.exists(index_name):
            responses.append(self.es.indices.delete(index=index_name))
        responses.append(self.es.indices.create(index=index_name, body=body))
        return responses

    def bulk_data_generator(self, index_name, doc_type, bulk_data):
        for ndc_record in bulk_data:
            yield {
                "_index": index_name,
                "_type": doc_type,
                "_source": ndc_record
            }

    def bulk_build_es_index(self, index_name, doc_type, bulk_data):
        if not self.es.indices.exists(index_name):
            self.create_es_index(index_name)
        r = helpers.bulk(
            self.es,
            self.bulk_data_generator(
                index_name=index_name,
                doc_type=doc_type,
                bulk_data=bulk_data
            )
        )
        return r

    def index_record(self, index_name, doc_type, doc):
        r = self.es.index(index=index_name, doc_type=doc_type, body=doc)
        return r

    def ndc_index_mapping(self, doc_type):
        # Turn this into something that pulls mappings from a dynamic online registry eventually
        create_index_request = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.mapping.ignore_malformed": True
            }
        }
        if doc_type == "ndc_collection_item" and 0 == 1:
            create_index_request["mappings"] = {
                    doc_type: {
                        "properties": {
                            "title": {
                                "type": "text"
                            },
                            "datasetreferencedate": {
                                "type": "text"
                            },
                            "ndc_collection_id": {
                                "type": "text"
                            },
                            "ndc_collection_title": {
                                "type": "text"
                            },
                            "ndc_collection_abstract": {
                                "type": "text"
                            },
                            "ndc_collection_owner": {
                                "type": "text"
                            },
                            "ndc_collection_link": {
                                "type": "text"
                            },
                            "ndc_collection_cached": {
                                "type": "date",
                                "format": "strict_date_optional_time"
                            },
                            "ndc_collection_created": {
                                "type": "date",
                                "format": "strict_date_optional_time"
                            },
                            "ndc_collection_last_updated": {
                                "type": "date",
                                "format": "strict_date_optional_time"
                            },
                            "ndc_file_date": {
                                "type": "date",
                                "format": "strict_date_optional_time"
                            },
                            "ndc_geopoint": {
                                "type": "geo_point"
                            }
                        }
                    }
                }

        return create_index_request

    def map_index(self, index_name, doc_type="ndc_collection_item"):
        mapping = self.ndc_index_mapping(doc_type=doc_type)
        r = self.es.indices.put_mapping(index=index_name, body=mapping, doc_type=doc_type)
        return r

    def enable_field_data(self, index_name, field_name, doc_type):
        mapping = {
            "properties": {
                field_name: {
                    "type": "text",
                    "fielddata": True
                }
            }
        }
        return self.es.indices.put_mapping(index=index_name, body=mapping, doc_type=doc_type)


class Storage:
    def __init__(self):
        self.aws = Connect()
        self.s3 = self.aws.aws_client("S3")
        self.s3_resource = self.aws.aws_client("S3", type="resource")

    def url_to_s3_key(self, url):
        parsed_url = urlsplit(url)
        return f"{parsed_url.netloc}{parsed_url.path}"

    def get_s3_file(self, key, bucket_name='ndc-collection-files', return_type='bytes'):
        self.s3.create_bucket(Bucket=bucket_name)

        try:
            bucket_object = self.s3.get_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            else:
                return str(e)

        if return_type == "raw":
            return bucket_object
        elif return_type == "bytes":
            return BytesIO(bucket_object['Body'].read())
        elif return_type == "dict":
            return json.loads(BytesIO(bucket_object['Body'].read()).read())
        elif return_type == "lines":
            try:
                return bucket_object['Body'].read().decode('utf-8').splitlines(True)
            except UnicodeDecodeError:
                return "File encoding problem encountered"

    def remove_s3_object(self, key, bucket_name='ndc-file-cache'):
        response = self.s3_resource.Object(bucket_name, key).delete()

        return response

    def transfer_file_to_s3(self, source_url, bucket_name="ndc-file-cache", key_name=None):
        if key_name is None:
            key_name = url_to_s3_key(source_url)

        self.s3.create_bucket(Bucket=bucket_name)
        bucket_object = self.s3_resource.Object(bucket_name, key_name)

        file_object = requests.get(source_url).content

        bucket_response = bucket_object.put(Body=file_object)

        return {
            "key_name": key_name,
            "source_url": source_url,
            "bucket_response": bucket_response
        }

    def put_json_to_s3(self, source_data, key_name, bucket_name):
        self.s3.create_bucket(Bucket=bucket_name)
        bucket_object = self.s3_resource.Object(bucket_name, key_name)
        bucket_response = bucket_object.put(Body=json.dumps(source_data))
        return bucket_response

    def check_s3_file(self, key_name, bucket_name):
        try:
            self.s3.Object(bucket_name, key_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return False
        else:
            return True

    def list_s3_keys(self, bucket_name):
        bucket_list = self.s3.list_objects(Bucket=bucket_name)
        if "Contents" in bucket_list.keys():
            return bucket_list['Contents']
        else:
            return list()


class Messaging:
    def __init__(self):
        aws = Connect()
        self.sqs = aws.aws_client("SQS")
        self.storage = Storage()

    def list_queues(self):
        queues = self.sqs.list_queues()
        return [q.split("/")[-1] for q in queues["QueueUrls"]]

    def get_message(self, QueueName):
        QueueUrl = self.sqs.create_queue(QueueName=QueueName)["QueueUrl"]

        response = self.sqs.receive_message(
            QueueUrl=QueueUrl,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        if "Messages" not in response.keys() or len(response['Messages']) == 0:
            return None

        message = {
            "ReceiptHandle": response['Messages'][0]['ReceiptHandle'],
            "Body": json.loads(response['Messages'][0]['Body'])
        }

        return message

    def post_message(self, QueueName, identifier, body):
        QueueUrl = self.sqs.create_queue(QueueName=QueueName)["QueueUrl"]

        response = self.sqs.send_message(
            QueueUrl=QueueUrl,
            MessageAttributes={
                'identifier': {
                    'DataType': 'String',
                    'StringValue': identifier
                }
            },
            MessageBody=(
                json.dumps(body)
            )
        )

        return response['MessageId']

    def delete_message(self, QueueName, ReceiptHandle):
        QueueUrl = self.sqs.create_queue(QueueName=QueueName)["QueueUrl"]

        self.sqs.delete_message(
            QueueUrl=QueueUrl,
            ReceiptHandle=ReceiptHandle
        )

        return ReceiptHandle

