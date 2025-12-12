# 🔌 API
> [🏠 Home](../README.md#-documentation-index) | [💽 Installation & Configuration](installation-and-configuring.md) | [⚙️ How it works](how-it-works.md) | [💬 FAQ](faq.md) 

### 📘 API Overview
| Operation Endpoints                                                                                   |
|-------------------------------------------------------------------------------------------------------|
| [Check DB connection](#check-db-connection)                                                           |
| [Run create-dict (scan) operation](#run-create-dict-scan-operation)                                   | 
| [Display database fields with anonymization rules](#display-database-fields-with-anonymization-rules) | 
| [Display table with anonymization data](#display-table-with-anonymization-data)                       | 
| [Run dump operation](#run-dump-operation)                                                             | 
| [Run restore operation](#run-restore-operation)                                                       | 

| Integration Endpoints                           |
|-------------------------------------------------| 
| [Operations list](#operations-list)             | 
| [Operation details](#operation-details)         | 
| [Delete operation data](#delete-operation-data) | 
| [Operation logs](#operation-logs)               | 

---

## Operation Endpoints

### Check DB connection
```http request
POST /api/stateless/check_db_connection
```

#### Description
Checks whether pg_anon can connect to the specified database.

If the connection is successful, the endpoint returns status code `200`.

#### 📦 Check DB connection request body schema
| Field         | Type    | Required | Description             |
|---------------|---------|----------|-------------------------|
| host          | string  | Yes      | Database host.          |
| port          | integer | Yes      | Database port.          |
| db_name       | string  | Yes      | Database name.          |
| user_login    | string  | Yes      | Database username.      |
| user_password | string  | Yes      | Database user password. |

#### Example
```shell
curl -X POST http://127.0.0.1:8000/api/stateless/check_db_connection \
-H "Content-Type: application/json" \
-d '{
  "host": "localhost",
  "port": "5432",
  "db_name":  "source_db",
  "user_login": "postgres",
  "user_password":  "postgres"
}'
```

#### ✅ Responses
| Status Code  | Description           | Component                                   |
|--------------|-----------------------|---------------------------------------------|
| `200`        | Database is reachable | -                                           |
| `400`        | Bad Request           | [ErrorResponse](#errorresponse)             |
| `500`        | Internal Server Error | [ErrorResponse](#errorresponse)             |
| `422`        | Validation Error      | [HTTPValidationError](#httpvalidationerror) |

---

### Run create-dict (scan) operation
```http request
POST /api/stateless/scan
```

#### Description
Runs pg_anon in [create-dict (scan) mode](operations/scan.md) in the background.

**Operation lifecycle:**
1. The client calls this endpoint.
2. The API returns one of the following status codes:
   - `200` — the scan operation has been successfully started.
   - `400` or `422` — the request is invalid; the operation is not started.
3. The service sends a webhook request with status `in_progress` to the `webhook_status_url`. The payload format is described in the [scan webhook request](#scan-webhook-request-schema) schema
4. The operation executes in the background.
5. If an error occurs during processing, the service sends a webhook request with status `error`.
6. If the operation completes successfully, the service sends a webhook request with status `success`.

#### 📦 Scan request body schema
| Field                 | Type                                              | Required | Description                                                                                                                                                  |
|-----------------------|---------------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation_id          | string                                            | Yes      | External operation ID. Accepts any string. Returned unchanged in webhook requests.                                                                           |
| db_connection_params  | [db connection params](#dbconnectionparams)       | Yes      | Source database connection credentials.                                                                                                                      |
| webhook_status_url    | string                                            | Yes      | Callback URL that receives POST requests in the format described in the [scan webhook request schema](#scan-webhook-request-schema).                         |
| webhook_metadata      | Any                                               | No       | Arbitrary metadata payload. Sent unchanged in webhook requests.                                                                                              |
| webhook_extra_headers | JSON                                              | No       | Additional HTTP headers added to webhook requests. Useful for integration, e.g., to include an `Authorization` header.                                       |
| webhook_verify_ssl    | boolean                                           | No       | Enables or disables SSL certificate verification for webhook requests.                                                                                       |
| save_dicts            | boolean                                           | No       | Saves all input and output dictionaries into the `runs` directory. Useful for debugging or integration. Default: `false`.                                    |
| type                  | string                                            | No       | Defines the scan mode: `full` or `partial`. Default: `partial`.                                                                                              |
| depth                 | integer                                           | No       | Maximum number of table rows used for partial scan. Applies only when `type = partial`. Default: `10000`.                                                    |
| meta_dict_contents    | array of [dictionary content](#dictionarycontent) | Yes      | Contents of the [meta dictionary](dicts/meta-dict-schema.md), defining rules for scanning fields.                                                            |
| sens_dict_contents    | array of [dictionary content](#dictionarycontent) | No       | Contents of the [sensitive dictionary](dicts/sens-dict-schema.md). Used to improve scan performance.                                                         |
| no_sens_dict_contents | array of [dictionary content](#dictionarycontent) | No       | Contents of the [non-sensitive dictionary](dicts/non-sens-dict-schema.md). Used to improve scan performance.                                                 |
| need_no_sens_dict     | boolean                                           | No       | If `true`, generates a [non-sensitive dictionary](dicts/non-sens-dict-schema.md) and returns it in the `no_sens_dict_contents` field of the webhook payload. |
| proc_count            | integer                                           | No       | Number of processes used for multiprocessing. Default: `4`.                                                                                                  |
| proc_conn_count       | integer                                           | No       | Number of database connections allocated per process for I/O operations. Default: `4`.                                                                       |

#### Example
```shell
curl -X POST http://127.0.0.1:8000/api/stateless/scan \
-H "Content-Type: application/json" \
-d '{
  "operation_id": "my-uniq-scan-id-0001",
  "db_connection_params": {
     "host": "localhost",
     "port": "5432",
     "db_name":  "source_db",
     "user_login": "postgres",
     "user_password":  "postgres"
  }
  "webhook_status_url": "https://my-service/pg-anon-result-processor",
  "webhook_metadata": {"extra_field_1": "extra_data", "extra_field_2": {"fld1": [1,2,3], "fld2": 123}},
  "webhook_extra_headers": {"Authorization": "Api-key my_super_secret_api_key", "X-my-service-extra-header": "header value"},
  "webhook_verify_ssl": true,
  "save_dicts": true,
  "type": "partial",
  "depth": 10000,
  "meta_dict_contents": [{
    "name": "my simple meta dict with email scan rule",
    "content": "{\"data_regex\": {\"rules\": [\".*@.*\"]}, \"funcs\": {\"text\": \"md5(%s)\"}}"
  }],
  "sens_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "no_sens_dict_contents": [{
    "name": "non-sens dict example",
    "content": "{\"no_sens_dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": [\"id\", \"created\"]}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": [\"id\", \"registered\"]}]}"
  }],
  "need_no_sens_dict": true,
  "proc_count": 4,
  "proc_conn_count": 4
}'
```

#### Scan webhook request schema
| Field                 | Type                                     | Required | Description                                                                                                                                                                                                                                                 |
|-----------------------|------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation_id          | string                                   | Yes      | External operation ID. Copied from the `operation_id` of the original scan request.                                                                                                                                                                         |
| internal_operation_id | string                                   | No       | Internal pg_anon operation ID generated automatically. Present only when `status` is `success` or `error`. Can be used to correlate the webhook event with operation data and logs stored in the `/runs` directory. Also used in the `Operation` endpoints. |
| status_id             | integer                                  | Yes      | Numeric status code. Possible values: `2` — success, `3` — error, `4` — in_progress.                                                                                                                                                                        |
| status                | string                                   | Yes      | Human-readable status. Possible values: `success`, `error`, `in_progress`.                                                                                                                                                                                  |
| webhook_metadata      | Any                                      | No       | Metadata payload passed “as is”. Copied from the `webhook_metadata` field of the scan request.                                                                                                                                                              |
| started               | string                                   | No       | Operation start timestamp in ISO8601 format (UTC+0). Present only for statuses `success` or `error`.                                                                                                                                                        |
| ended                 | string                                   | No       | Operation end timestamp in ISO8601 format (UTC+0). Present only for statuses `success` or `error`.                                                                                                                                                          |
| error                 | string                                   | No       | Error message. Present only when `status = error`.                                                                                                                                                                                                          |
| run_options           | JSON                                     | No       | Snapshot of the operation’s runtime options. Useful for analysis, debugging, and rerunning the operation. Present only when `status = success` or `error`.                                                                                                  |
| sens_dict_content     | [dictionary content](#dictionarycontent) | No       | Resulting [Sensitive dictionary](dicts/sens-dict-schema.md). Returned only when the scan completes successfully. Used for dump operations or repeated scans.                                                                                                |
| no_sens_dict_content  | [dictionary content](#dictionarycontent) | No       | Resulting [Non-Sensitive dictionary](dicts/non-sens-dict-schema.md). Present only when the scan completes successfully **and** the original request specified `need_no_sens_dict = true`.                                                                   |

#### ✅ Responses
| Status Code | Description                    | Component                                   |
|-------------|--------------------------------|---------------------------------------------|
| `201`       | Operation successfully started | -                                           |
| `400`       | Bad Request                    | [ErrorResponse](#errorresponse)             |
| `500`       | Internal Server Error          | [ErrorResponse](#errorresponse)             |
| `422`       | Validation Error               | [HTTPValidationError](#httpvalidationerror) |

---

### Display database fields with anonymization rules
```http request
POST /api/stateless/view-fields
```

#### Description
Runs pg_anon in [view-fields mode](operations/view-fields.md) and returns the result in the response.

#### 📦 View-fields request body schema
| Field                      | Type                                              | Required | Description                                                                                        |                             
|----------------------------|---------------------------------------------------|----------|----------------------------------------------------------------------------------------------------|
| db_connection_params       | [db connection params](#dbconnectionparams)       | Yes      | Source database credentials.                                                                       |
| sens_dict_contents         | array of [dictionary content](#dictionarycontent) | No       | [Sensitive dictionary](dicts/sens-dict-schema.md) content that defines rules for sensitive fields. |
| schema_name                | string                                            | No       | Filter by schema name.                                                                             |
| schema_mask                | string                                            | No       | Filter by schema name using a regular expression.                                                  |
| table_name                 | string                                            | No       | Filter by table name.                                                                              |
| table_mask                 | string                                            | No       | Filter by table name using a regular expression.                                                   |
| view_only_sensitive_fields | boolean                                           | No       | Displays only sensitive fields (default: `all fields`).                                           |
| fields_limit_count         | integer                                           | No       | Maximum number of fields to include for output (default: `5000`).                                  |

#### Example
```shell
curl -X POST http://127.0.0.1:8000/api/stateless/view-fields \
-H "Content-Type: application/json" \
-d '{
  "db_connection_params": {
     "host": "localhost",
     "port": "5432",
     "db_name":  "source_db",
     "user_login": "postgres",
     "user_password":  "postgres"
  }
  "sens_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "schema_name": "public",
  "table_mask": "^client",
  "view_only_sensitive_fields": true,
  "fields_limit_count": 1000,
}'
```

#### ✅ Responses
| Status Code   | Description           | Component                                   |
|---------------|-----------------------|---------------------------------------------|
| 200           | Successful Response   | [ViewFieldsResponse](#viewfieldsresponse)   |
| 400           | Bad Request           | [ErrorResponse](#errorresponse)             |
| 500           | Internal Server Error | [ErrorResponse](#errorresponse)             |
| 422           | Validation Error      | [HTTPValidationError](#httpvalidationerror) |

---

### Display table with anonymization data
```http request
POST /api/stateless/view-data
```

#### Description
Displays table data in original and anonymized variants for comparison.
Runs pg_anon in [view-data mode](operations/view-data.md) and returns the result in the response.

#### 📦 View-data request body schema
| Field                | Type                                              | Required | Description                                                                                    |
|----------------------|---------------------------------------------------|----------|------------------------------------------------------------------------------------------------|
| db_connection_params | [db connection params](#dbconnectionparams)       | Yes      | Source database credentials.                                                                   |
| sens_dict_contents   | array of [dictionary content](#dictionarycontent) | No       | [Sensitive dictionary](dicts/sens-dict-schema.md) content defining rules for sensitive fields. |
| schema_name          | string                                            | Yes      | Schema name.                                                                                   |
| table_name           | string                                            | Yes      | Table name.                                                                                    |
| limit                | integer                                           | No       | Number of rows to display (default: `100`).                                                    |
| offset               | integer                                           | No       | Row offset for pagination (default: `0`).                                                      |

#### Example
```shell
curl -X POST http://127.0.0.1:8000/api/stateless/view-data \
-H "Content-Type: application/json" \
-d '{
  "db_connection_params": {
     "host": "localhost",
     "port": "5432",
     "db_name":  "source_db",
     "user_login": "postgres",
     "user_password":  "postgres"
  }
  "sens_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "schema_name": "public",
  "table_name": "clients",
  "limit": 10,
  "offset": 20,
}'
```

#### ✅ Responses
| Status Code  | Description           | Component                                   |
|--------------|-----------------------|---------------------------------------------|
| 200          | Successful Response   | [ViewDataResponse](#viewdataresponse)       |
| 400          | Bad Request           | [ErrorResponse](#errorresponse)             |
| 500          | Internal Server Error | [ErrorResponse](#errorresponse)             |
| 422          | Validation Error      | [HTTPValidationError](#httpvalidationerror) |

---

### Run dump operation
```http request
POST /api/stateless/dump
```

#### Description
Runs pg_anon in [dump mode](operations/dump.md) in the background.

**Operation lifecycle:**
1. The client calls this endpoint.
2. The API returns one of the following status codes:
   - `200` — the dump operation has been successfully started.
   - `400` or `422` — the request is invalid; the operation is not started.
3. The service sends a webhook request with status `in_progress` to the `webhook_status_url`. The payload format is described in the [dump webhook request](#dump-webhook-request-schema) schema.
4. The operation executes in the background.
5. If an error occurs during processing, the service sends a webhook request with status `error`.
6. If the operation completes successfully, the service sends a webhook request with status `success`.

#### 📦 Dump request body schema
| Field                                | Type                                              | Required | Description                                                                                                                                                                                                                                                                                       |
|--------------------------------------|---------------------------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation_id                         | string                                            | Yes      | External operation ID. Accepts any string. Returned unchanged in webhook requests.                                                                                                                                                                                                                |
| db_connection_params                 | [db connection params](#dbconnectionparams)       | Yes      | Source database connection credentials.                                                                                                                                                                                                                                                           |
| webhook_status_url                   | string                                            | Yes      | Callback URL that receives POST requests in the format described in the [dump webhook request schema](#dump-webhook-request-schema).                                                                                                                                                              |
| webhook_metadata                     | Any                                               | No       | Arbitrary metadata payload. Sent unchanged in webhook requests.                                                                                                                                                                                                                                   |
| webhook_extra_headers                | JSON                                              | No       | Additional HTTP headers added to webhook requests. Useful for integration, e.g., to include an `Authorization` header.                                                                                                                                                                            |
| webhook_verify_ssl                   | boolean                                           | No       | Enables or disables SSL certificate verification for webhook requests.                                                                                                                                                                                                                            |
| save_dicts                           | boolean                                           | No       | Saves all input and output dictionaries into the `runs` directory. Useful for debugging or integration. Default: `false`.                                                                                                                                                                         |
| type                                 | string                                            | No       | Defines the dump type. Options: [`dump`](operations/dump.md#full-dump-dump-mode), [`sync-struct-dump`](operations/dump.md#structure-dump-sync-struct-dump-mode), [`sync-data-dump`](operations/dump.md#data-dump-sync-data-dump-mode). Default: [`dump`](operations/dump.md#full-dump-dump-mode). |
| sens_dict_contents                   | array of [dictionary content](#dictionarycontent) | Yes      | Contents of the [sensitive dictionary](dicts/sens-dict-schema.md), defining rules for data anonymization during the dump.                                                                                                                                                                         |
| partial_tables_dict_contents         | array of [dictionary content](#dictionarycontent) | No       | Contents of the [tables dictionary](dicts/tables-dictionary.md) specifying tables to **include** in a [partial dump](operations/dump.md#create-partial-dump).                                                                                                                                     |
| partial_tables_exclude_dict_contents | array of [dictionary content](#dictionarycontent) | No       | Contents of the [tables dictionary](dicts/tables-dictionary.md) specifying tables to **exclude** from a [partial dump](operations/dump.md#create-partial-dump).                                                                                                                                   |
| output_path                          | string                                            | No       | Path where the dump will be created under `/path/to/pg_anon/output`. For example, `"my_dump"` will be located at `/path/to/pg_anon/output/my_dump`.                                                                                                                                               |
| pg_dump_path                         | string                                            | No       | Path to the `pg_dump` Postgres tool. Default: `/usr/bin/pg_dump`.                                                                                                                                                                                                                                 |
| proc_count                           | integer                                           | No       | Number of processes used for multiprocessing. Default: `4`.                                                                                                                                                                                                                                       |
| proc_conn_count                      | integer                                           | No       | Number of database connections allocated per process for I/O operations. Default: `4`.                                                                                                                                                                                                            |
 
#### Example
```shell
curl -X POST http://127.0.0.1:8000/api/stateless/dump \
-H "Content-Type: application/json" \
-d '{
  "operation_id": "my-uniq-scan-id-0001",
  "db_connection_params": {
     "host": "localhost",
     "port": "5432",
     "db_name":  "source_db",
     "user_login": "postgres",
     "user_password":  "postgres"
  }
  "webhook_status_url": "https://my-service/pg-anon-result-processor",
  "webhook_metadata": {"extra_field_1": "extra_data", "extra_field_2": {"fld1": [1,2,3], "fld2": 123}},
  "webhook_extra_headers": {"Authorization": "Api-key my_super_secret_api_key", "X-my-service-extra-header": "header value"},
  "webhook_verify_ssl": true,
  "save_dicts": true,
  "type": "dump",
  "sens_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "partial_tables_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "partial_tables_exclude_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "output_path": "my_dump",
  "pg_dump_path": "/usr/lib/postgresql/17/bin/pg_dump"
  "proc_count": 4,
  "proc_conn_count": 4
}'
```

#### Dump webhook request schema
| Field                 | Type    | Required | Description                                                                                                                                                                                                                                                 |
|-----------------------|---------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation_id          | string  | Yes      | External operation ID. Copied from the `operation_id` of the original scan request.                                                                                                                                                                         |
| internal_operation_id | string  | No       | Internal pg_anon operation ID generated automatically. Present only when `status` is `success` or `error`. Can be used to correlate the webhook event with operation data and logs stored in the `/runs` directory. Also used in the `Operation` endpoints. |
| status_id             | integer | Yes      | Numeric status code. Possible values: `2` — success, `3` — error, `4` — in_progress.                                                                                                                                                                        |
| status                | string  | Yes      | Human-readable status. Possible values: `success`, `error`, `in_progress`.                                                                                                                                                                                  |
| webhook_metadata      | Any     | No       | Metadata payload passed “as is”. Copied from the `webhook_metadata` field of the scan request.                                                                                                                                                              |
| started               | string  | No       | Operation start timestamp in ISO8601 format (UTC+0). Present only for statuses `success` or `error`.                                                                                                                                                        |
| ended                 | string  | No       | Operation end timestamp in ISO8601 format (UTC+0). Present only for statuses `success` or `error`.                                                                                                                                                          |
| error                 | string  | No       | Error message. Present only when `status = error`.                                                                                                                                                                                                          |
| run_options           | JSON    | No       | Snapshot of the operation’s runtime options. Useful for analysis, debugging, and rerunning the operation. Present only when `status = success` or `error`.                                                                                                  |
| size                  | integer | No       | Size of the dump in bytes.                                                                                                                                                                                                                                  |

#### ✅ Responses
| Status Code | Description                    | Component                                   |
|-------------|--------------------------------|---------------------------------------------|
| `201`       | Operation successfully started | -                                           |
| `400`       | Bad Request                    | [ErrorResponse](#errorresponse)             |
| `500`       | Internal Server Error          | [ErrorResponse](#errorresponse)             |
| `422`       | Validation Error               | [HTTPValidationError](#httpvalidationerror) |

---

### Run restore operation
```http request
POST /api/stateless/restore
```

#### Description
Runs pg_anon in [restore mode](operations/restore.md) in the background.

**Operation lifecycle:**
1. The client calls this endpoint.
2. The API returns one of the following status codes:
   - `200` — the restore operation has been successfully started.
   - `400` or `422` — the request is invalid; the operation is not started.
3. The service sends a webhook request with status `in_progress` to the `webhook_status_url`. The payload format is described in the [restore webhook request](#restore-webhook-request-schema) schema.
4. The operation executes in the background.
5. If an error occurs during processing, the service sends a webhook request with status `error`.
6. If the operation completes successfully, the service sends a webhook request with status `success`.

#### 📦 Restore request body schema
| Field                                | Type                                              | Required | Description                                                                                                                                                                                                                                                                                       |
|--------------------------------------|---------------------------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation_id                         | string                                            | Yes      | External operation ID. Accepts any string. Returned unchanged in webhook requests.                                                                                                                                                                                                                |
| db_connection_params                 | [db connection params](#dbconnectionparams)       | Yes      | Source database connection credentials.                                                                                                                                                                                                                                                           |
| webhook_status_url                   | string                                            | Yes      | Callback URL that receives POST requests in the format described in the [restore webhook request schema](#restore-webhook-request-schema).                                                                                                                                                        |
| webhook_metadata                     | Any                                               | No       | Arbitrary metadata payload. Sent unchanged in webhook requests.                                                                                                                                                                                                                                   |
| webhook_extra_headers                | JSON                                              | No       | Additional HTTP headers added to webhook requests. Useful for integration, e.g., to include an `Authorization` header.                                                                                                                                                                            |
| webhook_verify_ssl                   | boolean                                           | No       | Enables or disables SSL certificate verification for webhook requests.                                                                                                                                                                                                                            |
| save_dicts                           | boolean                                           | No       | Saves all input and output dictionaries into the `runs` directory. Useful for debugging or integration. Default: `false`.                                                                                                                                                                         |
| type                                 | string                                            | No       | Defines the restore type. Options: [`restore`](operations/restore.md#full-restore-restore-mode), [`sync-struct-restore`](operations/restore.md#structure-restore-sync-struct-restore-mode), [`sync-data-restore`](operations/restore.md#data-restore-sync-data-restore-mode). Default: `restore`. |
| input_path                           | string                                            | Yes      | Path to the dump to restore, relative to `/path/to/pg_anon/output`. Example: `"my_dump"` will restore from `/path/to/pg_anon/output/my_dump`.                                                                                                                                                     |
| partial_tables_dict_contents         | array of [dictionary content](#dictionarycontent) | No       | Contents of the [tables dictionary](dicts/tables-dictionary.md) specifying tables to **include** in a [partial restore](operations/restore.md#create-partial-restore)                                                                                                                             |
| partial_tables_exclude_dict_contents | array of [dictionary content](#dictionarycontent) | No       | Contents of the [tables dictionary](dicts/tables-dictionary.md) specifying tables to **exclude** from a [partial restore](operations/restore.md#create-partial-restore)                                                                                                                           |
| pg_restore_path                      | string                                            | No       | Path to the `pg_restore` Postgres tool. Default: `/usr/bin/pg_restore`.                                                                                                                                                                                                                           |
| drop_custom_check_constr             | boolean                                           | No       | Drops all CHECK constraints that contain user-defined procedures to avoid performance degradation during data loading. Default: `false`.                                                                                                                                                          |
| clean_db                             | boolean                                           | No       | Cleans existing database objects before restoring. Mutually exclusive with `drop_db`.                                                                                                                                                                                                             |
| drop_db                              | boolean                                           | No       | Drops the target database before restoring. Mutually exclusive with `clean_db`.                                                                                                                                                                                                                   |
| proc_count                           | integer                                           | No       | Number of processes used for multiprocessing. Default: `4`.                                                                                                                                                                                                                                       |
| proc_conn_count                      | integer                                           | No       | Number of database connections allocated per process for I/O operations. Default: `4`.                                                                                                                                                                                                            |

#### Example
```shell
curl -X POST http://127.0.0.1:8000/api/stateless/restore \
-H "Content-Type: application/json" \
-d '{
  "operation_id": "my-uniq-scan-id-0001",
  "db_connection_params": {
     "host": "localhost",
     "port": "5432",
     "db_name":  "source_db",
     "user_login": "postgres",
     "user_password":  "postgres"
  }
  "webhook_status_url": "https://my-service/pg-anon-result-processor",
  "webhook_metadata": {"extra_field_1": "extra_data", "extra_field_2": {"fld1": [1,2,3], "fld2": 123}},
  "webhook_extra_headers": {"Authorization": "Api-key my_super_secret_api_key", "X-my-service-extra-header": "header value"},
  "webhook_verify_ssl": true,
  "save_dicts": true,
  "type": "restore",
  "input_path": "my_dump",
  "partial_tables_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "partial_tables_exclude_dict_contents": [{
    "name": "sens dict for email anonymization",
    "content": "{\"dictionary\": [{\"schema\": \"public\", \"table\": \"users\", \"fields\": {\"email\": \"md5(email)\"}}, {\"schema\": \"public\", \"table\": \"clients\", \"fields\": {\"email\": \"md5(email)\"}}]}"
  }],
  "pg_restore_path": "/usr/lib/postgresql/17/bin/pg_restore"
  "drop_custom_check_constr": false,
  "clean_db": false,
  "drop_db": false,
  "proc_count": 4,
  "proc_conn_count": 4
}'
```

#### Restore webhook request schema
| Field                 | Type    | Required | Description                                                                                                                                                                                                                                                 |
|-----------------------|---------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation_id          | string  | Yes      | External operation ID. Copied from the `operation_id` of the original scan request.                                                                                                                                                                         |
| internal_operation_id | string  | No       | Internal pg_anon operation ID generated automatically. Present only when `status` is `success` or `error`. Can be used to correlate the webhook event with operation data and logs stored in the `/runs` directory. Also used in the `Operation` endpoints. |
| status_id             | integer | Yes      | Numeric status code. Possible values: `2` — success, `3` — error, `4` — in_progress.                                                                                                                                                                        |
| status                | string  | Yes      | Human-readable status. Possible values: `success`, `error`, `in_progress`.                                                                                                                                                                                  |
| webhook_metadata      | Any     | No       | Metadata payload passed “as is”. Copied from the `webhook_metadata` field of the scan request.                                                                                                                                                              |
| started               | string  | No       | Operation start timestamp in ISO8601 format (UTC+0). Present only for statuses `success` or `error`.                                                                                                                                                        |
| ended                 | string  | No       | Operation end timestamp in ISO8601 format (UTC+0). Present only for statuses `success` or `error`.                                                                                                                                                          |
| error                 | string  | No       | Error message. Present only when `status = error`.                                                                                                                                                                                                          |
| run_options           | JSON    | No       | Snapshot of the operation’s runtime options. Useful for analysis, debugging, and rerunning the operation. Present only when `status = success` or `error`.                                                                                                  |

#### ✅ Responses
| Status Code | Description                    | Component                                   |
|-------------|--------------------------------|---------------------------------------------|
| `201`       | Operation successfully started | -                                           |
| `400`       | Bad Request                    | [ErrorResponse](#errorresponse)             |
| `500`       | Internal Server Error          | [ErrorResponse](#errorresponse)             |
| `422`       | Validation Error               | [HTTPValidationError](#httpvalidationerror) |

---

## Integration Endpoints

### Operations list
```http request
GET /operation
```

#### Description
Returns a list of background operation directories (scan, dump, restore). Only operations executed with `save_dicts` enabled are included. Useful for integration purposes.

#### 📦 Operations list request params
| Field        | Type | Required | Description                                               |
|--------------|------|----------|-----------------------------------------------------------|
| date_before  | date | No       | Filter: operations before this date. Date format ISO 8601 |
| date_after   | date | No       | Filter: operations after this date. Date format ISO 8601  |

#### Example
```shell
curl -X GET http://127.0.0.1:8000/operation?date_after=2025-01-01&date_before=2025-12-31
```

#### ✅ Responses
| Status Code   | Description             | Component                                   |
|---------------|-------------------------|---------------------------------------------|
| `200`         | List of operation paths | array of strings                            |
| `422`         | Validation Error        | [HTTPValidationError](#httpvalidationerror) |

---

## Operation details
```http request
GET /operation/{internal_operation_id}
```

#### Description
Returns detailed information about a background operation (scan, dump, restore). Only operations executed with `save_dicts` enabled are included. Useful for integration purposes.

#### 📦 Operations details request params
| Field                 | Type   | Required | Description                    |
|-----------------------|--------|----------|--------------------------------|
| internal_operation_id | string | Yes      | Internal pg_anon operation ID. |

#### Example
```shell
curl -X GET http://127.0.0.1:8000/operation/c6c98133-856f-46b3-ba9e-3a0092b8d9aa
```

### ✅ Responses
| Status Code | Description                   | Component                                       |
|-------------|-------------------------------|-------------------------------------------------|
| `200`       | Operation details             | [OperationDataResponse](#operationdataresponse) |
| `404`       | Operation directory not found | [HTTPValidationError](#httpvalidationerror)     |
| `422`       | Validation Error              | [HTTPValidationError](#httpvalidationerror)     |


---

### Delete operation data
```http request
DELETE /operation/{internal_operation_id}
```

#### Description
Deletes the operation data directory in `/runs`.
Also removes the dump directory from the output path if the operation type is `dump`.

#### Example
```shell
curl -X DELETE http://127.0.0.1:8000/operation/c6c98133-856f-46b3-ba9e-3a0092b8d9aa
```

#### 📦 Delete operation data request params
| Field                 | Type   | Required | Description                    |
|-----------------------|--------|----------|--------------------------------|
| internal_operation_id | string | Yes      | Internal pg_anon operation ID. |

### ✅ Responses
| Status Code    | Description                         | Component                                   |
|----------------|-------------------------------------|---------------------------------------------|
| `204`          | Operation data successfully deleted | -                                           |
| `400`          | Bad Request                         | [ErrorResponse](#errorresponse)             |
| `500`          | Internal Server Error               | [ErrorResponse](#errorresponse)             |
| `422`          | Validation Error                    | [HTTPValidationError](#httpvalidationerror) |

---

### Operation logs
```http request
GET /operation/{internal_operation_id}/logs
```

#### Description
Returns log output for a background operation (scan, dump, restore). Only operations executed with `save_dicts` enabled are included. Useful for integration purposes.

#### 📦 Operations logs request params
| Name                  | Type    | Required | Description                                                           |
|-----------------------|---------|----------|-----------------------------------------------------------------------|
| internal_operation_id | string  | Yes      | Internal pg_anon operation ID.                                        | 
| tail_lines            | integer | No       | Number of log lines to read from the end of the file. Default: `1000` |

#### Example
```shell
curl -X GET http://127.0.0.1:8000/operation/c6c98133-856f-46b3-ba9e-3a0092b8d9aa/logs
```

#### ✅ Responses
| Status Code | Description          | Component                                   |
|-------------|----------------------|---------------------------------------------|
| `200`       | Successful Response  | array of strings                            |
| `422`       | Validation Error     | [HTTPValidationError](#httpvalidationerror) |

---

# 📋 General schemas

## DbConnectionParams
| Field         | Type    | Required | Description             |
|---------------|---------|----------|-------------------------|
| host          | string  | Yes      | Database host.          |
| port          | integer | Yes      | Database port.          |
| db_name       | string  | Yes      | Database name.          |
| user_login    | string  | Yes      | Database user.          |
| user_password | string  | Yes      | Database user password. |

## DictionaryContent
| Field           | Type   | Required | Description                                                           |
|-----------------|--------|----------|-----------------------------------------------------------------------|
| name            | string | Yes      | Dictionary name. For example can be used as dictionary filename       |
| content         | string | Yes      | Dictionary content that using for operations processing               |
| additional_info | Any    | No       | Extra data for integration purposes. Will be sent on webhook "as is". |

## DictionaryMetadata
| Field           | Type   | Required | Description                                                           |
|-----------------|--------|----------|-----------------------------------------------------------------------|
| name            | string | Yes      | Dictionary name. For example can be used as dictionary filename       |
| additional_info | Any    | No       | Extra data for integration purposes. Will be sent on webhook "as is". |

## OperationDataResponse
| Field        | Type                                  | Required | Description                                                                                               |
|--------------|---------------------------------------|----------|-----------------------------------------------------------------------------------------------------------|
| run_status   | [RunStatus](#runstatus)               | Yes      | Operation status.                                                                                         |
| run_options  | JSON                                  | Yes      | Snapshot of the operation’s runtime options. Useful for analysis, debugging, and rerunning the operation. |
| dictionaries | [DictionariesData](#dictionariesdata) | Yes      | Used and resulted dictionary contents by types.                                                           |
| extra_data   | JSON                                  | No       | For dump operations contains dump size info. In other cases is empty.                                     |

## RunStatus
| Field     | Type    | Required | Description                                                       |
|-----------|---------|----------|-------------------------------------------------------------------|
| status_id | integer | Yes      | Numeric status code. Possible values: `2` — success, `3` — error. |
| status    | string  | Yes      | Human-readable status. Possible values: `success`, `error`.       |
| started   | string  | No       | Operation start timestamp in ISO8601 format (UTC+0).              |
| ended     | string  | No       | Operation end timestamp in ISO8601 format (UTC+0).                |

## DictionariesData
| Field                             | Type                                              | Required | Description                                                                           |
|-----------------------------------|---------------------------------------------------|----------|---------------------------------------------------------------------------------------|
| meta_dict_files                   | array of [dictionary content](#dictionarycontent) | No       | Contents of the **input** [meta dictionary](dicts/meta-dict-schema.md).               |
| prepared_sens_dict_files          | array of [dictionary content](#dictionarycontent) | No       | Contents of the **input** [sensitive dictionary](dicts/sens-dict-schema.md).          |
| prepared_no_sens_dict_files       | array of [dictionary content](#dictionarycontent) | No       | Contents of the **input** [non-sensitive dictionary](dicts/non-sens-dict-schema.md).  |
| partial_tables_dict_files         | array of [dictionary content](#dictionarycontent) | No       | Contents of the **input** [tables dictionary](dicts/tables-dictionary.md).            |  
| partial_tables_exclude_dict_files | array of [dictionary content](#dictionarycontent) | No       | Contents of the **input** [tables dictionary](dicts/tables-dictionary.md).            |  
| output_sens_dict_file             | [dictionary content](#dictionarycontent)          | No       | Contents of the **output** [sensitive dictionary](dicts/sens-dict-schema.md).         |
| output_no_sens_dict_file          | [dictionary content](#dictionarycontent)          | No       | Contents of the **output** [non-Sensitive dictionary](dicts/non-sens-dict-schema.md). |

## ViewDataResponse
| Field     | Type                                | Required | Description                                                          |
|-----------|-------------------------------------|----------|----------------------------------------------------------------------|
| status_id | integer                             | Yes      | Integer code of operation status. Can be: `2` - success, `3` - error |
| status    | string                              | Yes      | Human readable operation status. Can be: `success`, `error`          |
| content   | [ViewDataContent](#viewdatacontent) | No       | Operation result data                                                |

## ViewDataContent
| Field            | Type         | Required | Description                                                             |
|------------------|--------------|----------|-------------------------------------------------------------------------|
| schema_name      | string       | Yes      | Schema name                                                             |
| table_name       | string       | Yes      | Table name                                                              |
| field_names      | array        | Yes      | Table field names. It needs for rendering table header                  |
| total_rows_count | integer      | Yes      | Total rows count in table. It useful for pagination                     |
| rows_before      | array        | Yes      | Source rows data "as is" without anonymization                          |
| rows_after       | array        | Yes      | Anonymized rows, for display how anonymization will work on source data |

## ViewFieldsResponse
| Field     | Type                                    | Required | Description                                                          |
|-----------|-----------------------------------------|----------|----------------------------------------------------------------------|
| status_id | integer                                 | Yes      | Integer code of operation status. Can be: `2` - success, `3` - error |
| status    | string                                  | Yes      | Human readable operation status. Can be: `success`, `error`          |
| content   | [ViewFieldsContent](#viewfieldscontent) | No       | Operation result data                                                |

## ViewFieldsContent
| Field       | Type                                      | Required | Description                                               |
|-------------|-------------------------------------------|----------|-----------------------------------------------------------|
| schema_name | string                                    | Yes      | Schema name                                               |
| table_name  | string                                    | Yes      | Table name                                                |
| field_name  | string                                    | Yes      | Field name                                                |
| type        | string                                    | Yes      | Field data type                                           |
| dict_data   | [DictionaryMetadata](#dictionarymetadata) | No       | Matched dictionary metadata containing anonymization rule |
| rule        | str                                       | No       | Matched anonymization rule if field is sensitive          |

## ErrorResponse
| Field   | Type   | Required | Description    |
|---------|--------|----------|----------------|
| message | string | Yes      | Error message. |

## HTTPValidationError
| Field  | Type   | Required | Description    |
|--------|--------|----------|----------------|
| detail | array  | Yes      | Error details. |

## ValidationError
| Field   | Type   | Required | Description      |
|---------|--------|----------|------------------|
| loc     | array  | Yes      | Wrong parameter. |
| msg     | string | Yes      | Error message.   |
| type    | string | Yes      | Error type.      |
