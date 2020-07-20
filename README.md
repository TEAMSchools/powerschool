# PowerSchool
powerschool is a Python client for the [PowerSchool SIS](https://www.powerschool.com/solutions/student-information-system/powerschool-sis) API

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install powerschool.
```bash
pip install powerschool
```

## Getting Started
1. Ensure you have a valid [plugin](https://support.powerschool.com/developer/#/page/plugin-xml) installed with the proper data access provisioned for your purposes.
   
2. Instantiate the client by passing the host name of your server.
    ```python
    import powerschool
    ps = powerschool.PowerSchool('my.host.name')
    ```

3. Authorize the client using:
    - client credentials (tuple)
    ```python
    my_credentials = (client_id, client_secret)
    ps.authorize(client_credentials=my_credentials)
    ```

    - a previously saved access token (dict)
    ```python
    with open(token_file, 'r') as f:
        my_token = json.load(f)
    
    ps.authorize(access_token=my_token)
    ```

## Usage
>*Refer to the [docs](https://support.powerschool.com/developer/#/page/data-access) for full functionality, including resources, searching, and pagination.*

**Instantiate a table or PowerQuery object:**
```python
schools_table = ps.get_schema_table('schools')

powerquery = ps.get_named_query('com.pearson.core.student.search.get_student_basic_info')
```

**Get the record count for a table:**
```python
schools_table.count()
```

**Query all records, all columns on a table:**
>*Pagination is handled automatically by the client. However, you can manually pass `pagesize` and `page` parameters, should you choose.*
```python
schools_table.query()
```

**Query all records on a table, with filter and columns list:**
```python
params = {
    'q': 'id=ge=10000',
    'projection': 'school_number,abbreviation',
}
schools_table.query(**params)
```

**Query a specific record on a table:**
```python
schools_table.query(dcid=123)
```

**Execute a PowerQuery, passing arguments in the body:**
```python
payload = {
    'studentdcid': '5432',
}
powerquery.query(body=payload)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Notice
PowerSchool® is a registered trademark in the U.S. and/or other countries owned by PowerSchool Education, Inc. or its affiliates. PowerSchool® is used under license.
