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
- **region**: Region Name (string, default: ap-northeast-1)
- **table**: Table Name (string, required)
- **scan_limit**: DynamoDB 1time Scan Query size limit (Int, optional) 
- **record_limit**: Max Record Search limit (Long, optional) 
- **columns**: a key-value pairs where key is a column name and value is options for the column (required)
  - **name**: Column name.
  - **type**: Column values are converted to this embulk type.  
  Available values options are: `boolean`, `long`, `double`, `string`, `json`
- **filters**: query filter (optional)

## Example

```yaml
in:
  type: dynamodb
  auth_method: basic
  access_key: YOUR_ACCESS_KEY
  secret_key: YOUR_SECRET_KEY
  region: ap-northeast-1
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

## Build

```
$ ./gradlew gem
```
