#!/python3.6
from collections import namedtuple
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import requests
import os
import json
import datetime
import math

HTTP_ERROR = requests.exceptions.HTTPError

class Client:
    def __init__(self, base_url, client_id, client_secret, access_token_path='./.tokens'):
        """
        """
        self.base_url = base_url

        self.session = requests.Session()
        self.session.headers = self.authorize(client_id, client_secret, access_token_path)

        metadata_dict = self.get_metadata()
        Metadata = namedtuple('Metadata', sorted(metadata_dict))
        self.metadata = Metadata(**metadata_dict)

    def authorize(self, client_id, client_secret, access_token_path):
        """
        """
        access_token_file = f'{access_token_path}/access_token.json'

        if os.path.isfile(str(access_token_file)):
            # load cached access token and check for expiration
            print("Loading saved access token...")
            with open(access_token_file) as f:
                access_token_dict = json.load(f)

            expiration_timestamp = access_token_dict['expiration_timestamp']
            expiration_datetime = datetime.datetime.strptime(expiration_timestamp, '%Y-%m-%d %H:%M:%S.%f')

            # if expired fetch new access token
            if datetime.datetime.utcnow() > expiration_datetime:
                print("Access token expired...")
                access_token_dict = self.fetch_access_token(client_id, client_secret, access_token_file)

        elif client_id and client_secret:
            access_token_dict = self.fetch_access_token(client_id, client_secret, access_token_path)

        else:
            raise Exception("You must provide a valid access token or client credentials.")

        access_token = access_token_dict['access_token']
        token_type = access_token_dict['token_type']
        session_headers = {
                'Authorization': f'{token_type} {access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }

        return session_headers

    def fetch_access_token(self, client_id, client_secret, access_token_path):
        """
        """
        print("Generating new access token...")
        access_token_url = f'{self.base_url}/oauth/access_token'

        auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)

        access_token_dict = oauth.fetch_token(token_url=access_token_url, auth=auth)

        # calculate expiration datetime
        expires_in = int(access_token_dict['expires_in'])
        created_datetime = datetime.datetime.utcnow()
        expiration_datetime = created_datetime + datetime.timedelta(seconds=expires_in)
        expiration_timestamp = expiration_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        access_token_dict['expiration_timestamp'] = expiration_timestamp

        # save access token for future use
        if not os.path.exists(access_token_path):
            print(f"Creating folder {access_token_path}")
            os.makedirs(access_token_path)

        access_token_file = f'{access_token_path}/access_token.json'
        with open(access_token_file, 'w+') as f:
            json.dump(access_token_dict, f)

        return access_token_dict

    def _api_request(self, method, path, params=None, body=None):
        """
        """
        url = f'{self.base_url}{path}'

        try:
            response = self.session.request(method, url=url, params=params, data=body)
            response.raise_for_status()
            return response.json()
        except HTTP_ERROR as e:
            print(e)

            try:
                response_dict = response.json()
                print(f"\t{response_dict['message']}")
                for er in response_dict['errors']:
                    print(f"\t\t{er['resource']}: {er['field']} - {er['code']}")
            except:
                pass

            raise e

    def get_metadata(self):
        """
        """
        path = '/ws/v1/metadata'
        response_dict = self._api_request('GET', path)
        return response_dict['metadata']

    def schema_table_count(self, table_name, query=None):
        """
        """
        if query == '':
            query = None

        path = f'/ws/schema/table/{table_name}/count'
        query_params = {'q': query}
        count_response_dict = self._api_request('GET', path, query_params)
        return count_response_dict['count']

    def get_schema_table(self, table_name, id=None, query=None, page=None,
                            page_size=None, projection='*', students_to_include=None,
                            teachers_to_include=None, sort=None, sort_descending=None):
        """
        """
        if id:
            path = f'/ws/schema/table/{table_name}/{id}'
            query_params = {'projection': projection}
            query_response_dict = self._api_request('GET', path, query_params)

            return query_response_dict['tables'][table_name]

        else:
            if page_size == None:
                page_size = self.metadata.schema_table_query_max_page_size

            if query == '':
                query = None

            table_count = self.schema_table_count(table_name)
            if table_count > 0:
                path = f'/ws/schema/table/{table_name}'

                pages = math.ceil(table_count / page_size)

                query_results = []
                for p in range(pages):
                    query_params = {
                            'q': query,
                            'page': p + 1,
                            'pagesize': page_size,
                            'projection': projection,
                            'students_to_include': students_to_include,
                            'teachers_to_include': teachers_to_include,
                            'sort': sort,
                            'sort_descending': sort_descending,
                        }
                    query_response_dict = self._api_request('GET', path, query_params)

                    for r in query_response_dict['record']:
                        query_results.append(r['tables'][table_name])

                return query_results

    def put_schema_table(self, table_name, id, body):
        """
        """
        path = f'/ws/schema/table/{table_name}/{id}'
        return self._api_request('PUT', path, body)

    def delete_schema_table(self, table_name, id):
        """
        """
        path = f'/ws/schema/table/{table_name}/{id}'
        return self._api_request('DELETE', path)
