from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
import requests

from datetime import datetime
from collections import namedtuple
import math

class PowerSchool:
    def __init__(self, host):
        """
        """
        self.host = host
        self.base_url = f'https://{self.host}'
        self.access_token = None
        self.metadata = None        
        self.session = requests.Session()
        self.session.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
    
    def _request(self, method, path, params={}, data={}):
        """
        """        
        url = f'{self.base_url}{path}'        
        try:
            response = self.session.request(method, url=url, params=params, json=data)
            response.raise_for_status()            
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(e)
            try:
                response_dict = response.json()
                print(f"\t{response_dict.get('message')}")
                for er in response_dict.get('errors'):
                    print(f"\t\t{er.get('resource')}: {er.get('field')} - {er.get('code')}")
            except:
                pass            
            raise e
    
    def _metadata(self):
        """
        """
        path = '/ws/v1/metadata'        
        response_dict = self._request('GET', path)
        
        metadata_dict = response_dict.get('metadata')
        MetadataTuple = namedtuple('Metadata', sorted(metadata_dict))
        return MetadataTuple(**metadata_dict)
    
    def authorize(self, access_token=None, client_credentials=None):
        """
        """
        # check if access token supplied
        if access_token:
            # check if access token is still valid
            expires_at = datetime.fromtimestamp(access_token.get('expires_at'))
            now = datetime.now()
            if expires_at > now:
                self.access_token = access_token
                self.session.headers['Authorization'] = f"Bearer {self.access_token.get('access_token')}"                
                self.metadata = self._metadata()
                print("Authorized!")
                return
            else:
                print("Access token expired!")
        
        # check for client credentials (tuple)
        if isinstance(client_credentials, tuple):
            client_id, client_secret = client_credentials

            # fetch new access token
            print("Fetching new access token...")
            token_url = f'{self.base_url}/oauth/access_token/'
            auth = HTTPBasicAuth(client_id, client_secret)
            client = BackendApplicationClient(client_id=client_id)
            session = OAuth2Session(client=client)
            
            token_dict = session.fetch_token(token_url=token_url, auth=auth)

            self.access_token = token_dict
            self.session.headers['Authorization'] = f"Bearer {self.access_token.get('access_token')}"
            self.metadata = self._metadata()
            print("Authorized!")
            return
        else:
            # exit - prompt for credientials tuple
            raise Exception("You must provide a valid access token file or client credentials.")

    def count(self, count_type, name, body={}, **params):
        """
        """
        path = f'/ws/schema/{count_type}/{name}/count'
        
        if count_type == 'query':
            method = 'POST'
        else:
            method = 'GET'
        
        count_response_dict = self._request(method, path, params, body)
        return count_response_dict.get('count')

    def schema_table_metadata(self, table_name, **params):
        path = f'/ws/schema/table/{table_name}/metadata'
        return self._request('GET', path, params=params)
    
    def schema_table_query(self, table_name, row_id=None, page_size=None, projection=None, **params):
        """
        """
        path = f'/ws/schema/table/{table_name}'
        
        if projection is None:
            metadata_params = {'expansions': 'access'}
            table_metadata = self.schema_table_metadata(table_name, **metadata_params)
            table_columns = table_metadata.get('columns')
            star_projection = ','.join(
                [ c.get('name').lower() for c in table_columns if c.get('access') != 'NoAccess' ]
            )
            params.update({'projection': star_projection})
        else:
            params.update({'projection': projection})
        
        if row_id:
            path = f'{path}/{row_id}'
            response_dict = self._request('GET', path, params)
            return [response_dict.get('tables').get(table_name)]
        else:
            count_params = { k: params.get(k) for k in ['q', 'students_to_include', 'teachers_to_include'] }
            count = self.count('table', table_name, **count_params)
            if count > 0:
                if page_size is None:
                    page_size = self.metadata.schema_table_query_max_page_size
                params.update({'pagesize': page_size})
                pages = math.ceil(count / page_size)
                
                results = []
                for p in range(pages):
                    params.update({'page': p + 1})
                    response_dict = self._request('GET', path, params)
                    for r in response_dict.get('record'):
                        results.append(r.get('tables').get(table_name))
                return results
    
    def named_query(self, query_name, page_size=None, body={}, **params):
        """
        """
        path = f'/ws/schema/query/{query_name}'        
        count = self.count('query', query_name, body)
        if count > 0:
            if page_size is None:
                page_size = self.metadata.schema_table_query_max_page_size
            params.update({'pagesize': page_size})
            pages = math.ceil(count / page_size)

            results = []
            for p in range(pages):
                params.update({'page': p + 1})
                response_dict = self._request('POST', path, params, body)
                for r in response_dict.get('record'):
                    results.append(r)
            return results