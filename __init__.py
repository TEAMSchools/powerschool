from collections import namedtuple
import requests
import os
import json
import datetime
import base64
import urllib
import math

HTTP_ERROR = requests.exceptions.HTTPError

class Client:
    def __init__(self, base_url, client_id, client_secret):
        self.base_url = base_url

        self.session = requests.Session()
        self.session.headers = self.authorize(client_id, client_secret)

        self.metadata = self._metadata()

    def authorize(self, client_id=None, client_secret=None, access_token_file=None):
        if os.path.isfile(str(access_token_file)):
            # load cached access token and check for expiration
            with open(access_token_file) as f:
                access_token_json = json.load(f)

            expiration_timestamp = access_token_json['expiration_timestamp']
            expiration_datetime = datetime.datetime.strptime(expiration_timestamp, '%Y-%m-%d %H:%M:%S.%f')

            # if expired generate new access token
            if datetime.datetime.utcnow() > expiration_datetime:
                access_token_json = self.generate_access_token()
        elif client_id and client_secret:
            access_token_json = self.generate_access_token(client_id, client_secret)
        else:
            raise

        access_token = access_token_json['access_token']
        token_type = access_token_json['token_type']
        session_headers = {
                'Authorization': f'{token_type} {access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }

        return session_headers

    def generate_access_token(self, client_id, client_secret):
        credentials_concatenated = ':'.join((client_id, client_secret))
        credentials_encoded = base64.b64encode(credentials_concatenated.encode('utf-8'))

        access_url = f'{self.base_url}/oauth/access_token'
        access_headers = {
                'Authorization': b'Basic ' + credentials_encoded,
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            }
        access_params = {'grant_type': 'client_credentials'}

        print('Generating new access token...')
        access_response = self.session.post(access_url, headers=access_headers, params=access_params)

        access_token_json = access_response.json()

        # calculate expiration datetime
        expires_in = int(access_token_json['expires_in'])
        created_datetime = datetime.datetime.utcnow()
        expiration_datetime = created_datetime + datetime.timedelta(seconds=expires_in)
        expiration_timestamp = expiration_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        access_token_json['expiration_timestamp'] = expiration_timestamp

        # save access token for future use
        base_url_parsed = urllib.parse.urlparse(self.base_url)
        base_url_clean = base_url_parsed.netloc.replace('.', '_')
        access_token_filepath = f'./{base_url_clean}_access_token.json'

        with open(access_token_filepath, 'w+') as f:
            json.dump(access_token_json, f)

        return access_token_json

    def _api_request(self, method, path, params=None, body=None):
        url = f'{self.base_url}{path}'

        try:
            response = self.session.request(method, url=url, params=params, data=body)
            response.raise_for_status()

            return response.json()

        except HTTP_ERROR as e:
            print(e)

            try:
                response_json = response.json()
                print(f"\t{response_json['message']}")
                for er in response_json['errors']:
                    print(f"\t\t{er['resource']}: {er['field']} - {er['code']}")
            except:
                pass

            raise e

    def _metadata(self):
        path = '/ws/v1/metadata'
        response_json = self._api_request('GET', path)
        metadata_dict = response_json['metadata']

        Metadata = namedtuple('Metadata', sorted(metadata_dict))
        return Metadata(**metadata_dict)

    def table_count(self, table_name):
        path = f'/ws/schema/table/{table_name}/count'
        count_response_json = self._api_request('GET', path)
        return count_response_json['count']

    def schema_table_query(self, table_name, id=None, query=None, page=None, page_size=None,
                            projection='*', students_to_include=None, teachers_to_include=None,
                            sort=None, sort_descending=None):
        if id:
            path = f'/ws/schema/table/{table_name}/{id}'
            query_params = {'projection': projection}
            query_response_json = self._api_request('GET', path, query_params)
            return query_response_json['tables'][table_name]
        else:
            if page_size == None:
                page_size = self.metadata.schema_table_query_max_page_size

            if query == '':
                query = None

            table_count = self.table_count(table_name)
            if table_count > 0:
                path = f'/ws/schema/table/{table_name}'

                pages = math.ceil(table_count / page_size)

                query_result = []
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
                    query_response_json = self._api_request('GET', path, query_params)

                    for r in query_response_json['record']:
                        query_result.append(r['tables'][table_name])

                return query_result

    def put_schema_table_record(self, table_name, id, body):
        path = f'/ws/schema/table/{table_name}/{id}'
        return self._api_request('PUT', path, body)

    def delete_schema_table_record(self, table_name, id):
        path = f'/ws/schema/table/{table_name}/{id}'
        return self._api_request('DELETE', path)
