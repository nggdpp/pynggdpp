import os
import json
from elasticsearch import Elasticsearch
from .aws import Connect
from .serverful import Infrastructure
from collections import OrderedDict


class Mongo:
    def __init__(self):
        self.serverful_infrastructure = Infrastructure()

    def query_collections(self, q=None, ndc_collection_id=None, base_url=None):
        ndc_collections_db = self.serverful_infrastructure.connect_mongodb(collection="ndc_collections")

        if ndc_collection_id is not None:
            query = {
                "ndc_collection_id": ndc_collection_id
                }
        else:
            if q is None or q == "*":
                query = {}
            else:
                query = {
                    "$text":
                        {
                            "$search": q
                        }
                    }

        if base_url is None:
            reference_domain = "/"
        else:
            reference_domain = base_url

        recordset = list()
        for collection_record in list(ndc_collections_db.find(query)):
            if ndc_collection_id is None:
                collection_record["ndc_collection_link"] = f"{reference_domain}/{collection_record['ndc_collection_id']}"
            else:
                collection_record["ndc_collection_link"] = reference_domain
            del collection_record["_id"]
            recordset.append(collection_record)

        result_package = OrderedDict()
        result_package["total"] = len(recordset)
        result_package["selfLink"] = {
            "rel": "self",
            "url": base_url
        }
        result_package["collections"] = recordset

        return result_package

    def query_files(self, ndc_collection_id=None, base_url=None):
        ndc_files_db = self.serverful_infrastructure.connect_mongodb(collection="ndc_files")

        match_aggregation = {"$match":
                                 {
                                     "ndc_collection_id": ndc_collection_id
                                 }
                             }

        group_aggregation = {"$group":
                                {
                                    "_id": "$ndc_collection_id",
                                    "ndc_collection_title":
                                    {
                                        "$first": "$ndc_collection_title"
                                    },
                                    "ndc_collection_owner":
                                    {
                                        "$first": "$ndc_collection_owner"
                                    },
                                    "record_number":
                                    {
                                        "$sum": "$processing_metadata.accepted_record_number"
                                    },
                                    "files_processed":
                                    {
                                        "$addToSet": "$ndc_file_name"
                                    },
                                    "latest_file_date":
                                    {
                                        "$max": "$ndc_file_date"
                                    },
                                }
                            }

        if ndc_collection_id is not None:
            pipeline = [match_aggregation, group_aggregation]
        else:
            pipeline = [group_aggregation]

        if base_url is None:
            reference_domain = "/"
        else:
            reference_domain = base_url

        recordset = list()
        for collection_record in list(ndc_files_db.aggregate(pipeline)):
            collection_record["ndc_collection_id"] = collection_record["_id"]
            if ndc_collection_id is None:
                collection_record["ndc_collection_link"] = f"{reference_domain}/{collection_record['_id']}"
            else:
                collection_record["ndc_collection_link"] = reference_domain
            del collection_record["_id"]
            recordset.append(collection_record)

        result_package = OrderedDict()
        result_package["total"] = len(recordset)
        result_package["selfLink"] = {
                "rel": "self",
                "url": base_url
            }
        result_package["file_summary"] = recordset

        return result_package

    def query_organizations(self, base_url=None):
        pipeline = [
                        {"$group":
                            {
                                "_id": "$ndc_collection_owner",
                                "ndc_collection_owner_link": {"$first": "$ndc_collection_owner_link"},
                                "ndc_collection_owner_api": {"$first": "$ndc_collection_owner_api"},
                                "ndc_collection_owner_location": {"$first": "$ndc_collection_owner_location"},
                                "collections": {"$push": "$$ROOT"}
                            }
                        }
                    ]

        ndc_collections_db = self.serverful_infrastructure.connect_mongodb(collection="ndc_collections")

        recordset = list()
        for organization_record in list(ndc_collections_db.aggregate(pipeline)):
            org_collections = list()
            for collection_record in organization_record["collections"]:
                del collection_record["_id"]
                collection_record["ndc_collection_link"] = f"{base_url}/{collection_record['ndc_collection_id']}"
                org_collections.append(collection_record)
            org_record = {
                "ndc_collection_owner": organization_record["_id"],
                "ndc_collection_owner_link": organization_record["ndc_collection_owner_link"],
                "ndc_collection_owner_api": organization_record["ndc_collection_owner_api"],
                "ndc_collection_owner_location": organization_record["ndc_collection_owner_location"],
                "collections": org_collections
            }
            recordset.append(org_record)

        result_package = OrderedDict()
        result_package["total"] = len(recordset)
        result_package["selfLink"] = {
                "rel": "self",
                "url": base_url
            }
        result_package["collection_owners"] = recordset

        return result_package

    def query_items(self, q=None, first_id=None, last_id=None, limit=10, ndc_collection_id=None, base_url=None):
        ndc_items = self.serverful_infrastructure.connect_mongodb(collection="ndc_items")

        if not isinstance(limit, int):
            limit = 10

        if q == "*":
            q = None

        query = {}

        if ndc_collection_id is not None and q is None:
            query = {"ndc_collection_id": ndc_collection_id}

        if ndc_collection_id is None and q is not None:
            query = {"$text": {"$search": q}}

        if ndc_collection_id is not None and q is not None:
            query = {
                "$and": [
                    {"ndc_collection_id": ndc_collection_id},
                    {"$text": {"$search": q}}
                ]
            }

        if last_id is not None:
            query = {"_id": {"$gt": last_id}}
        if first_id is not None:
            query = {"_id": {"$lt": last_id}}

        data = list(ndc_items.find(query, {"_id": 0}).limit(limit))

        result_package = OrderedDict()
        result_package["total"] = len(data)
        result_package["selfLink"] = {
                "rel": "self",
                "url": base_url
            }
        result_package["items"] = data

        return result_package


class Search:
    def __init__(self):
        self.es = Elasticsearch(hosts=[os.environ["AWS_HOST_Elasticsearch"]])
        self.default_filter_path = 'hits'
        self.serverful_infrastructure = Infrastructure()

        self.query_all = {
            "query": {
                "match_all": {}
            }
        }

    def index_search(self, index_name, q, filter_path=None):
        query = {
            "query": {
                "query_string": {
                    "default_field": "*",
                    "query": q
                }
            }
        }
        return self.execute_query(index=index_name, query=query, filter_path=filter_path)

    def index_stats(self, index_name):
        simple_stats = {
            "index_exists": False
        }

        if index_name not in self.es.indices.get_alias().keys():
            return simple_stats
        else:
            simple_stats["index_exists"] = True
            stats = self.es.indices.stats(index_name)["indices"][index_name]["primaries"]
            simple_stats["doc_count"] = stats["docs"]["count"]
            simple_stats["size_in_bytes"] = stats["store"]["size_in_bytes"]
            return simple_stats

    def query_items(self, q=None, collection_id=None, size=20):
        if collection_id is None:
            index_name = "_all"
        else:
            index_name = collection_id

        if q is None or q == "*":
            query = {
                "query": {
                    "type": {
                        "value": "ndc_collection_item"
                    }
                }
            }
        else:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "type": {
                                    "value": "ndc_collection_item"
                                }
                            },
                            {
                                "multi_match": {
                                    "query": q,
                                    "fields": [
                                        "title^3",
                                        "abstract^2",
                                        "supplementalinformation",
                                        "supplementalinformation.info",
                                        "ndc_collection_abstract"
                                    ]
                                }
                            }
                        ]
                    }
                }
            }

        res = self.es.search(
            index=index_name,
            size=size,
            filter_path=['hits'],
            body=query
        )

        return res

    def package_collection_result(self, result_list, base_url="/"):
        recordset = list()
        for collection_record in result_list:
            del collection_record["_id"]
            collection_record["link"] = {
                    "rel": "self",
                    "url": f"{base_url}/{collection_record['ndc_collection_id']}"
                }
            recordset.append(collection_record)

        result_package = {
            "total": len(result_list),
            "selflink": {
                "rel": "self",
                "url": base_url
            },
            "collections": recordset
        }

        return result_package

    def query_collections(self, q=None, collection_id=None, size=20, base_url=None):
        index_name = "processed_collections"
        if collection_id is not None:
            query = {
                "query": {
                    "term": {
                        "_id": collection_id
                    }
                }
            }
        else:
            if q is None or q == "*":
                query = self.query_all
            else:
                query = {
                    "query": {
                        "multi_match": {
                            "query": q,
                            "fields": [
                                "collection_metadata.ndc_collection_title^3",
                                "collection_metadata.ndc_collection_abstract^2",
                                "collection_metadata.ndc_collection_owner"
                            ]
                        }
                    }
                }
        result = self.es.search(
            index=index_name,
            size=size,
            filter_path=['hits'],
            body=query
        )

        recordset = list()
        for collection in result["hits"]["hits"]:
            recordset.append(collection["_source"])

        return self.package_collection_result(result_list=recordset, base_url=base_url)

    def query_collections_all(self):
        return self.execute_query(index="processed_collections", size=1000, query=self.query_all)

    def query_collection_file_reports(self, ndc_collection_id, filter_path=None):
        query = {
            "query": {
                "match": {
                    "ndc_collection_id": ndc_collection_id
                }
            }
        }
        return self.execute_query(index="file_reports", query=query, filter_path=filter_path)

    def query_file_metadata(self, aws_s3_key, filter_path=None):
        query = {
            "query": {
                "term": {
                    "log_entry.aws_s3_key": aws_s3_key
                }
            }
        }
        return self.execute_query(index="processing_log", query=query, filter_path=filter_path)

    def execute_query(self, query, index, size=20, filter_path=None):
        if filter_path is None:
            filter_path = self.default_filter_path

        results = self.es.search(
            index=index,
            size=size,
            filter_path=filter_path,
            body=query
        )

        return results


class Maintenance:
    def __init__(self):
        aws_connect = Connect()
        self.sqs = aws_connect.aws_client("SQS")

    def list_queues(self):
        return self.sqs.list_queues()["QueueUrls"]

    def check_messages(self, QueueName):
        QueueUrl = self.sqs.create_queue(QueueName=QueueName)["QueueUrl"]

        response = self.sqs.receive_message(
            QueueUrl=QueueUrl,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        if "Messages" not in response.keys() or len(response['Messages']) == 0:
            return None

        return [json.loads(m["Body"]) for m in response["Messages"]]


