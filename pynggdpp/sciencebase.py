from datetime import datetime
import requests
from sciencebasepy import SbSession


class Organizations:
    def __init__(self):
        data = None

    def ndc_org(self, type='full', id=None):
        sb_collections = Collections()
        sb_api = f'{sb_catalog_path}?' \
                               f'format={sb_default_format}&' \
                               f'max=1000&' \
                               f'folderId={ndc_catalog_id}&' \
                               f"filter=tags%3D{sb_collections.ndc_collection_type_tag('ndc_organization',False)}"

        if id is not None and type != 'full':
            type = 'full'

        if type == 'full':
            sb_api = f"{sb_api}&fields=title,body,contacts,webLinks"
            if id is not None:
                sb_api = f"{sb_api}&id={id}"
        elif type == 'id':
            sb_api = f"{sb_api}&fields=id"

        sb_r = requests.get(sb_api).json()

        if type == 'full':
            items = list()
            for item in sb_r['items']:
                del item["link"]
                del item["relatedItems"]
                items.append(item)
            return items
        elif type == 'id':
            return [i['id'] for i in sb_r['items']]


class Collections:
    def __init__(self):
        self.sb = SbSession()
        self.sb_vocab_path = "https://www.sciencebase.gov/vocab"
        self.sb_party_root = "https://www.sciencebase.gov/directory/party/"
        self.sb_default_max = "100"
        self.sb_default_props = "title,body,contacts,spatial,files,webLinks,facets,dates,parentId"
        self.sb_files = Files()
        self.ndc_vocab_id = "5bf3f7bce4b00ce5fb627d57"
        self.ndc_catalog_id = "4f4e4760e4b07f02db47dfb4"

    def ndc_collection_type_tag(self, tag_name, include_type=True):
        vocab_search_url = f'{self.sb_vocab_path}/' \
                           f'{self.ndc_vocab_id}/' \
                           f'terms?nodeType=term&format=json&name={tag_name}'
        r_vocab_search = requests.get(vocab_search_url).json()
        if len(r_vocab_search['list']) == 1:
            tag = {'name': r_vocab_search['list'][0]['name'], 'scheme': r_vocab_search['list'][0]['scheme']}
            if include_type:
                tag['type'] = 'theme'
            return tag
        else:
            return None

    def ndc_collections(self, query=None):
        params = {
            'max': '100',
            'fields': 'title,body,contacts,spatial,files,webLinks,facets,dates,parentId',
            'folderId': '4f4e4760e4b07f02db47dfb4',
            'filter0': "tags={'name': 'ndc_collection', 'scheme': 'https://www.sciencebase.gov/vocab/category/NGGDPP/nggdpp_collection_types'}"
        }

        sb_collections = list()
        response = sb.find_items(params)
        while response and "items" in response:
            sb_collections.extend(response["items"])
            response = sb.next(response)

        nextLink = f'{self.sb_catalog_path}?' \
                               f'format={self.sb_default_format}&' \
                               f'max={self.sb_default_max}&' \
                               f'fields={self.sb_default_props}&' \
                               f'folderId={self.ndc_catalog_id}&' \
                               f"filter=tags%3D{self.ndc_collection_type_tag('ndc_collection',False)}"

        if query is not None:
            nextLink = f"{nextLink}&q={query}"

        collectionItems = list()

        while nextLink is not None:
            r_ndc_collections = requests.get(nextLink).json()

            if "items" in r_ndc_collections.keys():
                collectionItems.extend(r_ndc_collections["items"])

            if "nextlink" in r_ndc_collections.keys():
                nextLink = r_ndc_collections["nextlink"]["url"]
            else:
                nextLink = None

        if len(collectionItems) == 0:
            collectionItems = None
        else:
            # For some reason, the ScienceBase API is returning duplicate records.
            # This step gets unique IDs and then adds the first record for each to the array.
            unique_collections = list()
            for unique_id in list(set([i["id"] for i in collectionItems])):
                unique_collections.append(next(i for i in collectionItems if i["id"] == unique_id))
            collectionItems = unique_collections

        return collectionItems

    def ndc_collection_record(self, collection_id):
        r = requests.get(f"{self.sb_catalog_path}?"
                         f"id={collection_id}&"
                         f"format=json&"
                         f"fields={self.sb_default_props}"
                         ).json()

        if len(r["items"]) == 0:
            return None
        else:
            return r["items"][0]

    def ndc_collection_meta(self, collection_id=None, collection_record=None):
        if collection_record is None:
            collection_record = self.ndc_collection_record(collection_id)

        if collection_id is None:
            collection_id = collection_record["id"]

        collection_meta = dict()

        collection_meta["ndc_collection_id"] = collection_record["id"]
        collection_meta["ndc_collection_title"] = collection_record["title"]
        collection_meta["ndc_collection_link"] = collection_record["link"]["url"]
        collection_meta["ndc_collection_last_updated"] = next((d["dateString"] for d in collection_record["dates"]
                                                                            if d["type"] == "lastUpdated"), None)
        collection_meta["ndc_collection_created"] = next((d["dateString"] for d in collection_record["dates"]
                                                               if d["type"] == "dateCreated"), None)
        collection_meta["ndc_collection_cached"] = datetime.utcnow().isoformat()

        if "body" in collection_record.keys():
            collection_meta["ndc_collection_abstract"] = collection_record["body"]

        if "contacts" in collection_record.keys():
            data_owner_contact = next((c for c in collection_record["contacts"]
                                       if "type" in c.keys() and c["type"] == "Data Owner"), None)
        else:
            data_owner_contact = None

        if data_owner_contact is not None:
            collection_meta["ndc_collection_owner"] = data_owner_contact["name"]

            if "oldPartyId" in data_owner_contact.keys() and isinstance(data_owner_contact["oldPartyId"], int):
                collection_meta["ndc_collection_owner_api"] = \
                    f"{self.sb_party_root}{data_owner_contact['oldPartyId']}"

                r_sb_party = requests.get(f'{collection_meta["ndc_collection_owner_api"]}?format=json')
                if r_sb_party.status_code == 200:
                    sb_party = r_sb_party.json()
                    collection_meta["ndc_collection_owner_link"] = sb_party["url"]
                    collection_meta["ndc_collection_owner_location"] = \
                        f"{sb_party['primaryLocation']['mailAddress']['city']}, " \
                        f"{sb_party['primaryLocation']['mailAddress']['state']}"

            else:
                try:
                    collection_meta["ndc_collection_owner_link"] = data_owner_contact["onlineResource"]

                    collection_meta["ndc_collection_owner_location"] = \
                        f"{data_owner_contact['primaryLocation']['mailAddress']['city']}, " \
                        f"{data_owner_contact['primaryLocation']['mailAddress']['state']}"
                except KeyError:
                    pass

        return collection_meta


class Files:
    def __init__(self):
        self.acceptable_content_types = [
            'text/plain',
            'text/plain; charset=ISO-8859-1',
            'application/xml',
            'text/csv',
            'text/plain; charset=windows-1252'
        ]

    def actionable_files(self, sb_file_list):
        if sb_file_list is None:
            return None

        if len(sb_file_list) == 0:
            return None

        file_list = list()
        for file_obj in [f for f in sb_file_list if f["name"] != "metadata.xml"]:
            if "processed" in file_obj.keys():
                if file_obj["processed"]:
                    if file_obj["contentType"] in self.acceptable_content_types:
                        file_list.append(
                            {
                                "ndc_harvest_source": "ScienceBase",
                                "ndc_file_name": file_obj["name"],
                                "ndc_file_url": file_obj["url"],
                                "ndc_collection_id": file_obj["url"].split("/")[-1].split("?")[0],
                                "ndc_file_date": file_obj["dateUploaded"],
                                "ndc_file_size": file_obj["size"],
                                "ndc_file_content_type": file_obj["contentType"]
                            }
                        )
        if len(file_list) == 0:
            return None
        else:
            return file_list

    def uniform_file_object(self, harvest_url=None, file_metadata=None):
        file_object = dict()
        if harvest_url is not None:
            file_object["ndc_harvest_source"] = "waf"
            file_object["ndc_file_name"] = harvest_url["file_name"]
            file_object["ndc_file_url"] = harvest_url["file_url"]
            file_object["ndc_file_date"] = harvest_url["file_date"]
            try:
                file_object["ndc_file_size"] = int(harvest_url["file_size"])
            except:
                file_object["ndc_file_size"] = int(0)
            file_object["ndc_content_type"] = "application/xml"

        elif file_metadata is not None:
            file_object["ndc_harvest_source"] = "ScienceBase"
            file_object["ndc_file_name"] = file_metadata["name"]
            file_object["ndc_file_url"] = file_metadata["url"]
            file_object["ndc_file_date"] = file_metadata["dateUploaded"]
            file_object["ndc_file_size"] = int(file_metadata["size"])
            file_object["ndc_content_type"] = file_metadata["contentType"]
            if "processed" in file_metadata.keys():
                file_object["ndc_file_processed"] = file_metadata["processed"]
            else:
                file_object["ndc_file_processed"] = False

        return file_object

    def eval_file(self, sb_file_object):
        if sb_file_object["name"] != "metadata.xml" and ("contentType" in sb_file_object.keys()\
                and sb_file_object["contentType"] in self.acceptable_content_types)\
                and ("processed" in sb_file_object.keys() and sb_file_object["processed"]):
            return_file = self.uniform_file_object(file_metadata=sb_file_object)
            return_file["ndc_actionable"] = True

        else:
            return_file = sb_file_object
            return_file["ndc_actionable"] = False

        return return_file

