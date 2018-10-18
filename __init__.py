import os
import datetime
import requests
import json
import base64
import urllib
import pathlib
import math

class Client:
    def __init__(self, base_url, credentials):
        self.base_url = base_url

        self.session = requests.Session()
        self.session.headers = self.authorize(credentials)

        self.plugin_metadata = self.metadata()

    def generate_access_token(self, oauth_credentials):
        credentials_concatenated = ':'.join(oauth_credentials)
        credentials_encoded = base64.b64encode(credentials_concatenated.encode('utf-8'))

        access_url = f'{self.base_url}/oauth/access_token'
        access_headers = {
                'Authorization': b'Basic ' + credentials_encoded,
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            }
        access_params = { 'grant_type': 'client_credentials' }

        print('Generating new access token...')
        access_response = self.session.post(access_url, headers=access_headers, params=access_params)

        client_credentials = access_response.json()

        expires_in = int(client_credentials['expires_in'])
        created_datetime = datetime.datetime.utcnow()
        expiration_datetime = created_datetime + datetime.timedelta(seconds=expires_in)
        expiration_timestamp = expiration_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        client_credentials['expiration_timestamp'] = expiration_timestamp

        return client_credentials

    def authorize(self, credentials):
        if type(credentials) == tuple:
            # generate new access token
            client_credentials = self.generate_access_token(credentials)

            # save access token to .credentials for future use
            base_url_parsed = urllib.parse.urlparse(self.base_url)
            base_url_clean = base_url_parsed.netloc.replace('.', '_')
            credentials_filepath = f'./.credentials/{base_url_clean}_credentials.json'
            credentials_filepath_absolute = pathlib.Path(credentials_filepath).absolute()

            if not os.path.exists(credentials_filepath_absolute.parent):
                os.mkdir(credentials_filepath_absolute.parent)

            with open(credentials_filepath, 'w+') as f:
                json.dump(client_credentials, f)

        elif os.path.isfile(str(credentials)):
            # load cached access token and check for expiration
            with open(credentials) as f:
                client_credentials = json.load(f)

            expiration_timestamp = client_credentials['expiration_timestamp']
            expiration_datetime = datetime.datetime.strptime(expiration_timestamp, '%Y-%m-%d %H:%M:%S.%f')

            # if expired generate new access token
            if datetime.datetime.utcnow() > expiration_datetime:
                client_credentials = self.generate_access_token()

        else:
            raise

        access_token = client_credentials['access_token']
        token_type = client_credentials['token_type']
        session_headers = {
                'Authorization': f'{token_type} {access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }

        return session_headers

    def metadata(self):
        path = '/ws/v1/metadata'
        url = f'{self.base_url}{path}'

        response = self.session.get(url)
        response_json = response.json()

        return response_json['metadata']

    def table_count(self, table_name):
        path = f'/ws/schema/table/{table_name}/count'
        url = f'{self.base_url}{path}'

        count_response = self.session.get(url)
        count_response_json = count_response.json()

        return count_response_json['count']

    def schema_table_query(self, table_name, id=None, query=None, page=None, page_size=None,
                            projection='*', students_to_include=None, teachers_to_include=None,
                            sort=None, sort_descending=None):

        if id:
            path = f'/ws/schema/table/{table_name}/{id}'
            url = f'{self.base_url}{path}'
            query_params = {
                    'projection': projection,
                }

            query_response = self.session.get(url, params=query_params)
            query_result = query_response.json()
        else:
            path = f'/ws/schema/table/{table_name}'
            url = f'{self.base_url}{path}'
            if page_size == None:
                page_size = self.plugin_metadata['schema_table_query_max_page_size']
            if query == '':
                query = None

            query_result = []
            table_count = self.table_count(table_name)
            if table_count > 0:
                pages = math.ceil(table_count / page_size)
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

                    query_response = self.session.get(url, params=query_params)
                    query_response_json = query_response.json()
                    try:
                        records = query_response_json['record']
                    except:
                        print(query_response_json)
                        print(query_params)

                    for r in records:
                        query_result.append(r['tables'][table_name])

        return query_result

    def put_schema_table_record(self, table_name, id, body):
        path = f'/ws/schema/table/{table_name}/{id}'
        url = f'{self.base_url}{path}'
        response = self.session.put(url, data=body)
        return response.json()

    def delete_schema_table_record(self, table_name, id):
        path = f'/ws/schema/table/{table_name}/{id}'
        url = f'{self.base_url}{path}'
        response = self.session.delete(url)
        return response.json()