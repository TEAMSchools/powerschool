__version__ = "2.0.1"

from oauthlib.oauth2 import (
    BackendApplicationClient,
    TokenExpiredError,
    InvalidClientError,
)
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
import requests

from datetime import datetime
from collections import namedtuple
import math


class PowerSchool:
    """"""

    def __init__(self, host, auth):
        self.host = host
        self.base_url = f"https://{self.host}"
        self.access_token = None
        self.metadata = None
        self.session = requests.Session()
        self.session.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self.authorize(auth)

    def authorize(self, auth):
        """"""
        # check for access token (dict)
        if isinstance(auth, dict):
            # check if access token is still valid
            expires_at = datetime.fromtimestamp(auth.get("expires_at"))
            now = datetime.now()
            if expires_at > now:
                self.access_token = auth
                self.session.headers[
                    "Authorization"
                ] = f"Bearer {self.access_token.get('access_token')}"
                self.metadata = self._metadata()
                return "Authorized!"
            else:
                raise TokenExpiredError("Access token expired!")

        # check for client credentials (tuple)
        if isinstance(auth, tuple):
            client_id, client_secret = auth

            # fetch new access token
            token_url = f"{self.base_url}/oauth/access_token/"
            auth = HTTPBasicAuth(client_id, client_secret)
            client = BackendApplicationClient(client_id=client_id)
            session = OAuth2Session(client=client)

            token_dict = session.fetch_token(token_url=token_url, auth=auth)

            self.access_token = token_dict
            self.session.headers[
                "Authorization"
            ] = f"Bearer {self.access_token.get('access_token')}"
            self.metadata = self._metadata()
            return "Authorized!"
        else:
            raise InvalidClientError(
                "You must provide a valid access token file or client credentials."
            )

    def _request(self, method, path, params={}, data={}):
        """"""
        url = f"{self.base_url}{path}"
        response = self.session.request(method, url=url, params=params, json=data)
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(e)
            try:
                response_dict = response.json()
                print(f"\t{response_dict.get('message')}")
                for er in response_dict.get("errors"):
                    print(
                        f"\t\t{er.get('resource')}: {er.get('field')} - {er.get('code')}"
                    )
            except:
                pass
            raise e

    def _metadata(self):
        """"""
        path = "/ws/v1/metadata"
        response_dict = self._request("GET", path)

        metadata_dict = response_dict.get("metadata")
        MetadataTuple = namedtuple("Metadata", sorted(metadata_dict))
        return MetadataTuple(**metadata_dict)

    def get_schema_table(self, table_name):
        """"""
        self.schema_table = Schema(self, "table", table_name)
        return self.schema_table

    def get_named_query(self, query_name):
        """"""
        self.named_query = Schema(self, "query", query_name)
        return self.named_query


class Schema:
    """"""

    def __init__(self, client, schema_type, name):
        self.client = client
        self.schema_type = schema_type
        self.name = name

        if self.schema_type == "query":
            self.method = "POST"
        else:
            self.method = "GET"

    def count(self, body={}, **params):
        """"""
        path = f"/ws/schema/{self.schema_type}/{self.name}/count"
        filtered_params = {
            k: params.get(k)
            for k in ["q", "students_to_include", "teachers_to_include"]
        }
        count_response_dict = self.client._request(
            self.method, path, filtered_params, body
        )
        return count_response_dict.get("count")

    def metadata(self, **params):
        path = f"/ws/schema/{self.schema_type}/{self.name}/metadata"
        if self.schema_type == "query":
            return {}
        else:
            return self.client._request("GET", path, params=params)

    def query(self, pk=None, body={}, **params):
        """"""
        path = f"/ws/schema/{self.schema_type}/{self.name}"
        projection = params.get("projection")
        page = params.get("page")
        page_size = params.get("pagesize")

        if self.schema_type == "query":
            pass
        elif projection is None:
            metadata_params = {"expansions": "access"}
            metadata = self.metadata(**metadata_params)
            columns = metadata.get("columns")
            star_projection = ",".join(
                [
                    c.get("name").lower()
                    for c in columns
                    if c.get("access") not in ["NoAccess", "BlackListNoAccess"]
                ]
            )
            params.update({"projection": star_projection})
        else:
            params.update({"projection": projection})

        if pk:
            path = f"{path}/{pk}"
            filtered_params = {k: params.get(k) for k in ["projection"]}
            response_dict = self.client._request("GET", path, filtered_params)
            return [response_dict.get("tables").get(self.name)]
        else:
            count = self.count(body, **params)
            if count > 0:
                if page_size is None and self.schema_type == "query":
                    page_size = 0
                elif page_size is None:
                    page_size = self.client.metadata.schema_table_query_max_page_size
                params.update({"pagesize": page_size})

                if (
                    (page_size is None and self.schema_type == "query")
                    or (page_size == 0)
                    or (page)
                ):
                    pages = 1
                else:
                    pages = math.ceil(count / page_size)

                results = []
                for p in range(pages):
                    if page:
                        params.update({"page": page})
                    else:
                        params.update({"page": p + 1})

                    response_dict = self.client._request(
                        self.method, path, params, body
                    )
                    for r in response_dict.get("record"):
                        if self.schema_type == "query":
                            results.append(r)
                        else:
                            results.append(r.get("tables").get(self.name))
                return results
