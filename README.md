# Dynamodb input plugin for Embulk

## Overview

* **Plugin type**: input
* **Load all or nothing**: yes
* **Resume supported**: no
* **Cleanup supported**: no


## Configuration
- **auth_method**: AWS Credential Type.  
Available values options are: `basic`, `env`, `instance`, `profile`, `properties`
  - **basic**: AWS access key and secret access key
  - **env**: Environment variables
  - **instance**: EC2 Instance Metadata Service
  - **profile**: Profile configuration file
  - **properties**: Java system properties
- If **auth_method** is set `basic`
  - **access_key**: AWS access key (string, required)
  - **secret_key**: AWS secret key (string, required)
- If **auth_method** is set `profile`
  - **profile_name**: The name of a local configuration profile (string, optional)
- **region**: Region Name (string, optional)
- **end_point**: EndPoint URL (string, optional)  
`end_point` has priority when `region` and `end_point` are specified.
- **operation**: Operation Type (string, required)  
Available types are: `scan`, `query`
- **table**: Table Name (string, required)
- **filters**: Query Filters  
Required to `query` operation. Optional for `scan`.  
  - **name**: Column name.
  - **type**: Column type.
  - **condition**: Comparison Operator.
  - **value(s)**: Attribute Value(s).
- **limit**: DynamoDB 1-time Scan/Query Operation size limit (Int, optional)
- **scan_limit**: DynamoDB 1-time Scan Query size limit (Deprecated, Int, optional)
- **record_limit**: Max Record Search limit (Long, optional)
- **columns**: a key-value pairs where key is a column name and value is options for the column (required)
  - **name**: Column name.
  - **type**: Column values are converted to this embulk type.  
  Available values options are: `boolean`, `long`, `double`, `string`, `json`

## Example

- Scan Operation

```yaml
in:
  type: dynamodb
  auth_method: basic
  access_key: YOUR_ACCESS_KEY
  secret_key: YOUR_SECRET_KEY
  region: ap-northeast-1
  operation: scan
  table: YOUR_TABLE_NAME
  columns:
    - {name: ColumnA, type: long}
    - {name: ColumnB, type: double}
    - {name: ColumnC, type: string}
    - {name: ColumnD, type: boolean}
    - {name: ColumnE, type: json}  # DynamoDB Map, List and Set Column Type are json.
  filters:
    - {name: ColumnA, type: long, condition: BETWEEN, value: 10000, value2: 20000}
    - {name: ColumnC, type: string, condition: EQ, value: foobar}

out:
  type: stdout
```

- Query Operation

```yaml
in:
  type: dynamodb
  auth_method: env
  region: ap-northeast-1
  operation: query
  table: YOUR_TABLE_NAME
  columns:
    - {name: ColumnA, type: long}
    - {name: ColumnB, type: double}
    - {name: ColumnC, type: string}
    - {name: ColumnD, type: boolean}
    - {name: ColumnE, type: json}
  filters:
    - {name: ColumnA, type: long, condition: EQ, value: 10000}

out:
  type: stdout
```

## Try

```
$ ./gradlew classpath
$ embulk preview -I lib your-config.yml
```

## Run tests

```
$ ./test/run_dynamodb_local.sh
$ AWS_ACCESS_KEY_ID=${YOUR_AWS_ACCESS_KEY_ID} \
  AWS_SECRET_ACCESS_KEY=${YOUR_AWS_SECRET_ACCESS_KEY} \
  EMBULK_DYNAMODB_TEST_ACCESS_KEY=${YOUR_AWS_ACCESS_KEY_ID} \
  EMBULK_DYNAMODB_TEST_SECRET_KEY=${YOUR_AWS_SECRET_ACCESS_KEY} \
  EMBULK_DYNAMODB_TEST_PROFILE_NAME=${YOUR_AWS_PROFILE} \
  ./gradlew test
```

If you do not have any real aws account, you can skip the tests that use the real aws account.

```
$ RUN_AWS_CREDENTIALS_TEST=false ./gradlew test
```

## Build

```
$ ./test/run_dynamodb_local.sh
$ ./gradlew gem
```
