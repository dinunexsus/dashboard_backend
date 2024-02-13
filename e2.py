from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout, NotFoundError, ElasticsearchException
from datetime import datetime, timedelta, timezone
import logging
from abc import ABC, abstractmethod
import pytz



class ElasticsearchBackend(ABC):
    def __init__(self, hosts):
        self.es = self.connect_to_elasticsearch(hosts)
    
    def connect_to_elasticsearch(self, hosts):
        try:
            es = Elasticsearch(hosts, timeout=1300)
            if es.ping():
                logging.info("Connected to Elasticsearch.")
                return es
            else:
                logging.error("Elasticsearch connection failed.")
                return None
        except Exception as e:
            logging.error(f"Error connecting to Elasticsearch: {e}")
            return None
    
    @abstractmethod
    def get_alerts(self, responder_name, start_date=None, end_date=None):
        pass
    
    @abstractmethod
    def get_unique_responder_names(self):
        pass

    def get_unique_responder_names(self):
        query = {
            "size": 0,
            "aggs": {
                "unique_responder_names": {
                    "composite": {
                        "size": 10000,
                        "sources": [{"responder_name": {"terms": {"field": "parsedMessage.attributes.responders.name.keyword"}}}]
                    }
                }
            }
        }
        try:
            response = self.es.search(index="entity.alert", body=query)
            buckets = response.get('aggregations', {}).get('unique_responder_names', {}).get('buckets', [])
            unique_names = [bucket['key']['responder_name'] for bucket in buckets if '@' not in bucket['key']['responder_name']]
            return unique_names
        except ConnectionTimeout:
            logging.error("Connection timeout when fetching unique responder names.")
        except NotFoundError:
            logging.error("Index not found when fetching unique responder names.")
        except ElasticsearchException as e:
            logging.error(f"Unexpected Elasticsearch error: {e}")
        except Exception as e:
            logging.error(f"General error when fetching unique responder names: {e}")
        return []
    
    def format_date(self, date, time):
        formatted_date = date.replace("-", "/")
        datetime_str = f"{formatted_date} {time}"
        return datetime_str
    
    def format_for_es(self, date, time):
       
        formatted_date = date.replace("-", "/")
        
        datetime_str = f"{formatted_date} {time}"
        return datetime_str




    
    def get_alerts(self, responder_name, start_date=None, end_date=None, start_time="00:00:00", end_time="23:59:59"):
        # Check if start_date or end_date is None, default to current date
        if not start_date:
            start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if not end_date:
            end_date = start_date

        # Format start and end datetime using the provided date and time
        formatted_start_datetime = self.format_for_es(start_date, start_time)
        formatted_end_datetime = self.format_for_es(end_date, end_time)

        
       

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"parsedMessage.attributes.responders.name.keyword": responder_name}},
                        {"range": {
                            "parsedMessage.attributes.createdAtTime": {
                                "gte": formatted_start_datetime,
                                "lt": formatted_end_datetime
                            }
                        }}
                    ]
                }
            }
        }
        #     "query": {
        #         "bool": {
        #             "must": [
        #                 {"match": {"parsedMessage.attributes.responders.name": responder_name}},
        #                 {"range": {
        #                     "parsedMessage.attributes.createdAtTime": {
        #                         "gte": formatted_start_datetime,
        #                         "lt": formatted_end_datetime
        #                     }
        #                 }}
        #             ]
        #         }
        #     }
        # }
        logging.info(f"query:{query}")
        
        all_alerts = []
        try:
            page = self.es.search(index="entity.alert", body=query, scroll='1m', size=100)
            scroll_id = page['_scroll_id']
            alerts = [hit["_source"] for hit in page['hits']['hits']]
            while len(page['hits']['hits']):
                page = self.es.scroll(scroll_id=scroll_id, scroll='1m')
                scroll_id = page['_scroll_id']
                alerts.extend([hit["_source"] for hit in page['hits']['hits']])
            all_alerts.extend(alerts)
        except Exception as e:
            logging.error(f"Error fetching alerts: {e}")

        if 'scroll_id' in locals():
            self.es.clear_scroll(scroll_id=scroll_id)

        return all_alerts
    
    # def format_date(self, date_str):
    #     # Convert ISO 8601 format to the expected Elasticsearch date format
    #     # Assuming Elasticsearch expects "yyyy/MM/dd HH:mm:ss"
    #     date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    #     return date_obj.strftime("%Y/%m/%d %H:%M:%S")
    
   

    
    # def get_alerts(self, responder_name, start_datetime=None, end_datetime=None):
    #     # Assume start_datetime and end_datetime are now passed in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)

    #     if not start_datetime:
    #         start_datetime = datetime.utcnow().isoformat() + "Z"  # Default to current time if not provided
    #     if not end_datetime:
    #         end_datetime = start_datetime  # Use start_datetime as end_datetime if not provided
        
    #     formatted_start_datetime = self.format_date(start_datetime)
    #     formatted_end_datetime = self.format_date(end_datetime)
    #     query = {
    #         "query": {
    #             "bool": {
    #                 "must": [
    #                     {"match": {"parsedMessage.attributes.responders.name": responder_name}},
    #                     {"range": {
    #                         "parsedMessage.attributes.createdAtTime": {
    #                             "gte": formatted_start_datetime,
    #                             "lt": formatted_end_datetime
    #                         }
    #                     }}
    #                 ]
    #             }
    #         }
    #     }
     
        
        

    #     all_alerts = []
    #     try:
    #         page = self.es.search(index="entity.alert", body=query, scroll='1m', size=100)
    #         scroll_id = page['_scroll_id']
    #         alerts = [hit["_source"] for hit in page['hits']['hits']]
    #         while len(page['hits']['hits']):
    #             page = self.es.scroll(scroll_id=scroll_id, scroll='1m')
    #             scroll_id = page['_scroll_id']
    #             alerts.extend([hit["_source"] for hit in page['hits']['hits']])
    #         all_alerts.extend(alerts)
    #     except Exception as e:
    #         logging.error(f"Error fetching alerts: {e}")

    #     if 'scroll_id' in locals():
    #         self.es.clear_scroll(scroll_id=scroll_id)

    #     return all_alerts

    
    
    
    def flatten_json(self, y):
        out = {}
        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
        flatten(y)
        return out

    

    def convert_milliseconds_to_datetime(self, milliseconds):
        # Convert milliseconds to UTC datetime
        utc_datetime = datetime.utcfromtimestamp(milliseconds / 1000.0)
        # Convert UTC datetime to IST (UTC+5:30)
        ist_datetime = utc_datetime + timedelta(hours=5, minutes=30)
        return ist_datetime.strftime('%Y/%m/%d %H:%M:%S')

    def convert_milliseconds_to_minutes(self, milliseconds):
        return milliseconds / (1000.0 * 60)

    def is_milliseconds(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False
        
    def calculate_minutes_between_datetimes(self, datetime_str1, datetime_str2):
        dt_format = '%Y/%m/%d %H:%M:%S'
        datetime1 = datetime.strptime(datetime_str1, dt_format)
        datetime2 = datetime.strptime(datetime_str2, dt_format)
        delta = datetime2 - datetime1
        return delta.total_seconds() / 60

    def map_field_names(self, flattened_alert):
        readable_alert = {}
        direct_mappings = {
            'parsedMessage_attributes_cluster': 'Cluster',
            'parsedMessage_attributes_service': 'Service',
            'parsedMessage_attributes_priority': 'Priority',
            'parsedMessage_attributes_alertType': 'AlertType',
            'parsedMessage_attributes_message': 'AlertName',
            'parsedMessage_attributes_status': 'Status',
            'parsedMessage_attributes_createdAt': 'CreatedAt',
            'parsedMessage_attributes_updatedAt': 'UpdatedAt',
            'parsedMessage_attributes_severity': 'Severity',
            'parsedMessage_attributes_acknowledged': 'Acknowledged',
            'parsedMessage_attributes_alertAckTime': 'AlertAckTime',
            'parsedMessage_attributes_alertCloseTime': 'AlertCloseTime',
            'parsedMessage_attributes_acknowledgedBy': 'AckBy',
            'parsedMessage_attributes_closedBy': 'ClosedBy',
            'parsedMessage_attributes_tinyId': 'TinyID',
            'parsedMessage_attributes_responders_0_name': 'Team', 
            'parsedMessage_attributes_alertId': 'AlertID',
            'parsedMessage_attributes_runbook_url': 'RunbookUrl',
            'parsedMessage_attributes_zoneId': 'Zone',
            'parsedMessage_attributes_timeTakenToClose': 'TimeToClose',
            'parsedMessage_attributes_bu': 'BU',
            'parsedMessage_attributes_count':'count'
  
        }

     
        for key, value in flattened_alert.items():
            new_key = direct_mappings.get(key)
            if new_key:
                if new_key in ['CreatedAt', 'UpdatedAt', 'AlertCloseTime', 'AlertAckTime'] and self.is_milliseconds(value):
                    readable_alert[new_key] = self.convert_milliseconds_to_datetime(float(value))
                elif new_key == 'TimeToClose' and self.is_milliseconds(value):
                    readable_alert[new_key] = self.convert_milliseconds_to_minutes(float(value))
                else:
                    readable_alert[new_key] = value

    
        responder_fields = [
            ('parsedMessage_attributes_responders_0_onCalls_0_contacts_0_emailId', 'PrimaryResponderEmail'),
            ('parsedMessage_attributes_responders_0_onCalls_1_contacts_0_emailId', 'SecondaryResponderEmail'),
        ]

        for field_key, readable_key in responder_fields:
            if field_key in flattened_alert:
                readable_alert[readable_key] = flattened_alert[field_key]


        if 'CreatedAt' in readable_alert and 'AlertAckTime' in readable_alert and readable_alert.get('Acknowledged'):
            try:
                readable_alert['TimeToAck'] = self.calculate_minutes_between_datetimes(readable_alert['CreatedAt'], readable_alert['AlertAckTime'])
            except Exception as e:
                logging.error(f"Error calculating TimeToAck: {e}")
                readable_alert['TimeToAck'] = None
        
        tag_keys = [key for key in flattened_alert if key.startswith('parsedMessage_attributes_tags_')]
        tags = [flattened_alert[key] for key in tag_keys if key in flattened_alert]
        readable_alert['Tags'] = ', '.join(tags)

        required_fields_with_defaults = {
            'Cluster': 'Notfound',  
            'Zone': 'Notfound',    
            'Acknowledge': 'false'
        }

        for field, default_value in required_fields_with_defaults.items():
            if field not in readable_alert:
              
                readable_alert[field] = default_value

        alert_id = readable_alert.get('AlertID')
        if alert_id:
            readable_alert['AlertURL'] = f"https://zeta.app.opsgenie.com/alert/detail/{alert_id}/details"


        return readable_alert
