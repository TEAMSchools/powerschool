import math
from datetime import datetime

import requests
from oauthlib.oauth2 import (
    BackendApplicationClient,
    InvalidClientError,
    TokenExpiredError,
)
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session


class PowerSchool:
    """"""

    def __init__(self, host, **kwargs):
        self.host = host
        self.base_url = f"https://{self.host}"
        self.access_token = None
        self.metadata = None
        self.session = requests.Session()
        self.session.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        auth = kwargs.get("auth")
        if auth:
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
                self.metadata = self.get_plugin_metadata()
                return True
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
            self.metadata = self.get_plugin_metadata()
            return True
        else:
            raise InvalidClientError(
                "You must provide a valid access token file or client credentials."
            )

    def _request(self, method, path, params={}, data={}):
        """"""
        url = f"{self.base_url}/{path}"
        response = self.session.request(method, url=url, params=params, json=data)
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as xc:
            try:
                error_json = response.json()
                error_msg = error_json.get("message")
                errors = error_json.get("errors")
                errors_str = [
                    f"\t{er.get('resource')}: {er.get('field')} - {er.get('code')}\n"
                    for er in errors
                ]
                raise xc(f"{error_msg}\n{errors_str}")
            except:
                pass
            raise xc

    def get_plugin_metadata(self):
        """"""
        path = "ws/v1/metadata"
        response_dict = self._request("GET", path)

        metadata_dict = response_dict.get("metadata")
        return ClientMetadata(metadata_dict)

    def get_schema_table(self, table_name):
        """"""
        self.schema_table = SchemaTable(self, table_name, "table")
        return self.schema_table

    def get_named_query(self, query_name):
        """"""
        self.named_query = Schema(self, query_name, "query")
        return self.named_query


class ClientMetadata:
    """"""

    def __init__(self, metadata):
        for k, v in metadata.items():
            setattr(self, k, v)


class Schema:
    """
    A table object. Currently limited to extended schema tables, table extensions,
    child tables, and standalone tables.
    """

    def __init__(self, client, name, schema_type):
        self.client = client
        self.name = name
        self.schema_type = schema_type

        self.path = f"ws/schema/{self.schema_type}/{self.name}"

        if self.schema_type == "query":
            self.query_method = "POST"
        else:
            self.query_method = "GET"

    def metadata(self, **kwargs):
        """
        Get metadata for a table.
        """
        return self.client._request(
            method="GET",
            path=f"{self.path}/metadata",
            params=kwargs,
        )

    def count(self, **kwargs):
        """
        Calculates a row count for a table based on given criteria and returns it.
        """
        body = kwargs.pop("body", {})
        params = {
            k: kwargs.get(k)
            for k in ["q", "students_to_include", "teachers_to_include"]
        }
        response = self.client._request(
            method=self.query_method,
            path=f"{self.path}/count",
            params=params,
            data=body,
        )
        return response.get("count")

    def query(self, **kwargs):
        """
        Performs a query on a table and returns either a single row or paged results.
        """
        pk = kwargs.pop("pk")
        body = kwargs.pop("body", {})

        projection = kwargs.get("projection")
        page_size = kwargs.get("pagesize")
        page = kwargs.get("page")

        # auto-populate projection, if not provided
        if self.schema_type == "query":
            pass
        elif projection is None:
            metadata = self.metadata(expansions="access")
            columns = metadata.get("columns")
            all_columns = ",".join(
                [
                    c.get("name").lower()
                    for c in columns
                    if c.get("access") not in ["NoAccess", "BlackListNoAccess"]
                ]
            )
            kwargs.update({"projection": all_columns})

        # auto-populate page size, if not provided
        if page_size is None:
            if self.schema_type == "query":
                page_size = 1
            else:
                page_size = self.client.metadata.schema_table_query_max_page_size

            kwargs.update({"pagesize": page_size})

        # query single record
        if pk:
            response = self.client._request(
                method="GET",
                path=f"{self.path}/{pk}",
                params={"projection": projection},
            )
            return [response.get("tables").get(self.name)]
        # query multiple records
        else:
            count = self.count(**kwargs)
            data = []

            if count > 0:
                if page or page_size == 0:
                    n_pages = 1
                else:
                    n_pages = math.ceil(count / page_size)

                params = {
                    k: kwargs.get(k)
                    for k in [
                        "q",
                        "projection",
                        "page",
                        "pagesize",
                        "students_to_include",
                        "teachers_to_include",
                        "sort",
                        "sortdescending",
                        "extensions",
                    ]
                }

                for p in range(n_pages):
                    if page is None:
                        params.update({"page": p + 1})

                    response = self.client._request(
                        method=self.query_method,
                        path=self.path,
                        params=params,
                        data=body,
                    )

                for r in response.get("record"):
                    if self.schema_type == "query":
                        data.append(r)
                    else:
                        data.append(r.get("tables").get(self.name))

            return data


class SchemaTable(Schema):
    """
    A table object. Currently limited to extended schema tables, table extensions,
    child tables, and standalone tables.
    """

    def __init__(self, client, name, schema_type):
        super().__init__(client, name, schema_type)

    def insert(self, pk, body):
        """
        Insert a single row into a table.
        """
        return self.client._request(
            method="POST",
            path=f"{self.path}/{pk}",
            data=body,
        )

    def update(self, pk, body):
        """
        Update a single row in a table.
        """
        return self.client._request(
            method="PUT",
            path=f"{self.path}/{pk}",
            data=body,
        )

    def delete(self, pk):
        """
        Delete a single row in a table. Only available to extension tables and
        a few core tables.
        """
        return self.client._request(
            method="DELETE",
            path=f"{self.path}/{pk}",
        )
