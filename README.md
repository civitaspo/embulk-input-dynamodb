# Dynamodb input plugin for Embulk

![Master CI Status Badge](https://github.com/lulichn/embulk-input-dynamodb/workflows/Master%20CI/badge.svg) ![Test CI Status Badge](https://github.com/lulichn/embulk-input-dynamodb/workflows/Test%20CI/badge.svg)

## Overview

* **Plugin type**: input
* **Load all or nothing**: yes
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration
- **auth_method**: name of mechanism to authenticate requests (`"basic"`, `"env"`, `"instance"`, `"profile"`, `"properties"`, `"anonymous"`, `"session"`, `"web_identity_token"`, default: `"default"`)
  - `"basic"`: uses **access_key_id** and **secret_access_key** to authenticate.
  - `"env"`: uses `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`) and `AWS_SECRET_KEY` (or `AWS_SECRET_ACCESS_KEY`) environment variables.
  - `"instance"`: uses EC2 instance profile or attached ECS task role.
  - `"profile"`: uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.
    ```
    [default]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY

    [profile2]
    ...
    ```
  - `"properties"`: uses aws.accessKeyId and aws.secretKey Java system properties.
  - `"anonymous"`: uses anonymous access. This auth method can access only public files.
  - `"session"`: uses temporary-generated **access_key_id**, **secret_access_key** and **session_token**.
  - `"assume_role"`: uses temporary-generated credentials by assuming **role_arn** role.
  - `"web_identity_token"`: uses temporary-generated credentials by assuming **role_arn** role with web identity.
  - `"default"`: uses AWS SDK's default strategy to look up available credentials from runtime environment. This method behaves like the combination of the following methods.
    1. `"env"`
    1. `"properties"`
    1. `"web_identity_token"`
    1. `"profile"`
    1. `"instance"`
- **profile_file**: path to a profiles file. this is optionally used when **auth_method** is `"profile"`. (string, default: given by `AWS_CREDENTIAL_PROFILES_FILE` environment variable, or ~/.aws/credentials).
- **profile_name**: name of a profile. this is optionally used when **auth_method** is `"profile"`. (string, default: `"default"`)
- **access_key_id**: aws access key id. this is required when **auth_method** is `"basic"` or `"session"`. (string, optional)
  - You can use this option as **access_key**, but this name is deprecated.
- **secret_access_key**: aws secret access key. this is required when **auth_method** is `"basic"` or `"session"`. (string, optional)
  - You can use this option as **secret_key**, but this name is deprecated.
- **session_token**: aws session token. this is required when **auth_method** is `"session"`. (string, optional)
- **role_arn**: arn of the role to assume. this is required for **auth_method** is `"assume_role"` or `"web_identity_token"`. (string, optional)
- **role_session_name**: an identifier for the assumed role session. this is required when **auth_method** is `"assume_role"` or `"web_identity_token"`. (string, optional)
- **role_external_id**: a unique identifier that is used by third parties when assuming roles in their customers' accounts. this is optionally used for **auth_method**: `"assume_role"`. (string, optional)
- **role_session_duration_seconds**: duration, in seconds, of the role session. this is optionally used for **auth_method**: `"assume_role"`. (int, optional)
- **web_identity_token_file**: the absolute path to the web identity token file. this is required when **auth_method** is `"web_identity_token"`. (string, optional)
- **scope_down_policy**: an iam policy in json format. this is optionally used for **auth_method**: `"assume_role"`. (string, optional)
- **endpoint**: The AWS Service endpoint (string, optional)
  - You can use this option as **end_point**, but this name is deprecated.
- **region**: The AWS region (string, optional)
- **http_proxy**: Indicate whether using when accessing AWS via http proxy. (optional)
  - **host** proxy host (string, required)
  - **port** proxy port (int, optional)
  - **protocol** proxy protocol (string, default: `"https"`)
  - **user** proxy user (string, optional)
  - **password** proxy password (string, optional)
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

## Development

### Run examples

```shell
$ ./run_dynamodb_local.sh
$ ./example/prepare_dynamodb_table.sh
$ ./gradlew classpath
$ embulk run example/config.yml -Ilib
```

### Run tests

```shell
## Run dynamodb-local
$ ./run_dynamodb_local.sh
$ AWS_ACCESS_KEY_ID=${YOUR_AWS_ACCESS_KEY_ID} \
    AWS_SECRET_ACCESS_KEY=${YOUR_AWS_SECRET_ACCESS_KEY} \
    EMBULK_DYNAMODB_TEST_ACCESS_KEY=${YOUR_AWS_ACCESS_KEY_ID} \
    EMBULK_DYNAMODB_TEST_SECRET_KEY=${YOUR_AWS_SECRET_ACCESS_KEY} \
    EMBULK_DYNAMODB_TEST_PROFILE_NAME=${YOUR_AWS_PROFILE} \
    EMBULK_DYNAMODB_TEST_ASSUME_ROLE_ROLE_ARN=${YOUR_ROLE_ARN} \
    ./gradlew test
```

If you do not have any real aws account, you can skip the tests that use the real aws account.

```shell
$ ./run_dynamodb_local.sh
$ RUN_AWS_CREDENTIALS_TEST=false ./gradlew test
```

### Run the formatter

```shell
## Just check the format violations
$ ./gradlew spotlessCheck

## Fix the all format violations
$ ./gradlew spotlessApply
```

### Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

### Release gem:
Fix [build.gradle](./build.gradle), then


```shell
$ ./gradlew gemPush
```

## ChangeLog

[CHANGELOG.md](./CHANGELOG.md)

## License

[MIT LICENSE](./LICENSE)
