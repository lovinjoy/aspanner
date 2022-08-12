# aspanner
Asyncio Google Cloud Spanner Client, wrapped **google-cloud-spanner** to support aio calls, provide easy-to-use methods.
This project exists because Spanner have no easy-to-use asyncio interface.


References:

* https://github.com/googleapis/python-spanner
* https://googleapis.dev/python/spanner/latest/index.html
* https://cloud.google.com/spanner/docs/samples
* https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1


## Quick Start

1. Get the credentials JSON file from Google Cloud - IAM - Service Account - Keys,
or run directly from permission granted VM, follow the tutorials from google.

2. Create Spanner instance, create database, and create test table.

    ``` SQL
    CREATE TABLE tb_test_types (
        id INT64 NOT NULL,
        data_str STRING(MAX),
        data_int INT64,
        data_float FLOAT64,
        data_bool BOOL,
        data_num NUMERIC,
        data_bytes BYTES(MAX),
        data_date DATE,
        data_time TIMESTAMP,
        data_array ARRAY<INT64>,
        data_json JSON,
    ) PRIMARY KEY(id);
    ```

3. Code test.py

    ``` python
    async def test():
        db = aspanner.Aspanner('google cloud project id', 'spanner instance name', 'database')

        cols = ('id', 'data_int')
        print(await db.insert('tb_test_types', cols, [(9, 999)]))
        print(await db.read('tb_test_types', cols, [(9,)]))
        print(await db.delete('tb_test_types', [(9,)]))

        print(await db.close())

    asyncio.run(test())
    ```

4. Run in terminal.

    ``` bash
    pip install aspanner

    # if use credentials file
    export GOOGLE_APPLICATION_CREDENTIALS="/home/user/project-server-1234568-1234567890.json"

    python3 test.py
    ```

## TODO

    Retry now should be outside transaction with block, like: https://github.com/googleapis/google-cloud-python/blob/92465cbc4d9c0ba251838e9cd17f61d14b470e04/spanner/google/cloud/spanner_v1/session.py#L353

    error in sql while JSON is array.
