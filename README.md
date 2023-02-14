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
- **profile_file**: path to a profile file. this is optionally used when **auth_method** is `"profile"`. (string, default: given by `AWS_CREDENTIAL_PROFILES_FILE` environment variable, or ~/.aws/credentials).
- **profile_name**: name of a profile. this is optionally used when **auth_method** is `"profile"`. (string, default: `"default"`)
- **access_key_id**: aws access key id. this is required when **auth_method** is `"basic"` or `"session"`. (string, optional)
- **secret_access_key**: aws secret access key. this is required when **auth_method** is `"basic"` or `"session"`. (string, optional)
- **session_token**: aws session token. this is required when **auth_method** is `"session"`. (string, optional)
- **role_arn**: arn of the role to assume. this is required for **auth_method** is `"assume_role"` or `"web_identity_token"`. (string, optional)
- **role_session_name**: an identifier for the assumed role session. this is required when **auth_method** is `"assume_role"` or `"web_identity_token"`. (string, optional)
- **role_external_id**: a unique identifier that is used by third parties when assuming roles in their customers' accounts. this is optionally used for **auth_method**: `"assume_role"`. (string, optional)
- **role_session_duration_seconds**: duration, in seconds, of the role session. this is optionally used for **auth_method**: `"assume_role"`. (int, optional)
- **web_identity_token_file**: the absolute path to the web identity token file. this is required when **auth_method** is `"web_identity_token"`. (string, optional)
- **scope_down_policy**: an iam policy in json format. this is optionally used for **auth_method**: `"assume_role"`. (string, optional)
- **endpoint**: The AWS Service endpoint (string, optional)
- **region**: The AWS region (string, optional)
- **http_proxy**: Indicate whether using when accessing AWS via http proxy. (optional)
  - **host** proxy host (string, required)
  - **port** proxy port (int, optional)
  - **protocol** proxy protocol (string, default: `"https"`)
  - **user** proxy user (string, optional)
  - **password** proxy password (string, optional)
- **scan**: scan operation configuration. This option cannot be used with **query** option. (See [Operation Configuration Details](#operation-configuration-details), optional)
- **query**: query operation configuration. This option cannot be used with **scan** option. (See [Operation Configuration Details](#operation-configuration-details), optional)
- **table**: Table Name (string, required)
- **default_timestamp_format**: Format of the timestamp if **columns.type** is `"timestamp"`. (string, optional, default: `"%Y-%m-%d %H:%M:%S.%N %z"`)
- **default_timezone**: Time zone of timestamp columns if the value itself doesn’t include time zone description (eg. Asia/Tokyo). (string, optional, default: `"UTC"`)
- **default_date**: Set date part if the format doesn’t include date part. (string, optional, default: `"1970-01-01"`)
- **columns**: a key-value pairs where key is a column name and value is options for the column. If you do not specify this option, each dynamodb items are processed as a single json. (array of string-to-string map, optional, default: `[]`)
  - **name**: Name of the column. (string, required)
  - **type**: Embulk Type of the column that is converted to from dynamodb attribute value as possible. (`"boolean"`, `"long"`,`"timestamp"`, `"double"`, `"string"` or `"json"`, required)
  - **attribute_type**: Type of the Dynamodb attribute that name matches **name** of the column. The types except specified one are stored as `null` when this option is specified. (`"S"`, `"N"`, `"B"`, `"SS"`, `"NS"`, `"BS"`, `"M"`, `"L"`, `"NULL"` or `"BOOL"`, optional)
  - **format**: Format of the timestamp if **type** is `"timestamp"`. (string, optional, default value is specified by **default_timestamp_format**)
  - **timezone**: Timezone of the timestamp if the value itself doesn’t include time zone description (eg. Asia/Tokyo). (string, optional, default value is specified by **default_timezone**)
  - **date**: Set date part if the **format** doesn’t include date part. (string, optional, default value is specified by **default_date**)
- **json_column_name**: Name of the column when each dynamodb items are processed as a single json. (string, optional, default: `"record"`)

### Operation Configuration Details

Here is the explanation of the configuration for **scan** option or **query** option. The configuration has common options and specific options. Sometimes a type called `DynamodbAttributeValue` appears, see the end of this section first if you are worried about it.

#### Common Options

- **consistent_read**: Require strongly consistent reads or not. ref.  (boolean, optional, default: `false`)
  - See the docs ([Read Consistency for Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ReadConsistency) or [Read Consistency for Query](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.ReadConsistency)) for more details.
- **exclusive_start_key**: When you want to read the middle of the table, specify the attribute as the start key. (string to `DynamodbAttributeValue` map, optional)
  - See the docs ([Paginating Table Scan Results](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination) or [Paginating Table Query Results](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html)) for more details.
- **expression_attribute_names**: An expression attribute name is a placeholder that you use in an Amazon DynamoDB expression as an alternative to an actual attribute name. An expression attribute name must begin with a pound sign (#), and be followed by one or more alphanumeric characters. (string to string map, optional, default: `{}`)
  - See the doc ([Expression Attribute Names](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html)) for more details.
- **expression_attribute_values**: If you need to compare an attribute with a value, define an expression attribute value as a placeholder. Expression attribute values in Amazon DynamoDB are substitutes for the actual values that you want to compare—values that you might not know until runtime. An expression attribute value must begin with a colon (:) and be followed by one or more alphanumeric characters. (string to `DynamodbAttributeValue` map, optional, default: `{}`)
  - See the doc ([Expression Attribute Values](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html)) for more details.
- **filter_expression**: A filter expression is applied after the operation finishes, but before the results are returned. Therefore, the operation consumes the same amount of read capacity, regardless of whether a filter expression is present. (string, optional)
  - See the docs ([Filter Expressions for Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.FilterExpression) or [Filter Expressions for Query](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.FilterExpression)) for more details.
- **index_name**: Amazon DynamoDB provides fast access to items in a table by specifying primary key values. However, many applications might benefit from having one or more secondary (or alternate) keys available, to allow efficient access to data with attributes other than the primary key. To address this, you can create one or more secondary indexes on a table and issue **query** or **scan** operations against these indexes. (string, optional)
  - See the doc ([Improving Data Access with Secondary Indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html)) for more details.
- **batch_size**: The limit of items by an operation. The final result contains specified number of items or fewer when **filter_expression** is specified. (int, optional)
  - See the docs ([Limiting the Number of Items in the Result Set for Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Limit) or [Limiting the Number of Items in the Result Set for Query](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Limit)) for more details.
- **limit**: The limit of total items by operations. (long, optional)
- **projection_expression**: To read data from a table, you use operations. Amazon DynamoDB returns all the item attributes by default. To get only some, rather than all of the attributes, use a projection expression. (string, optional)
  - See the doc ([Projection Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html)) for more details.
- **select**: The attributes to be returned in the result. You can retrieve all item attributes, specific item attributes, the count of matching items, or in the case of an index, some or all of the attributes projected into the index. (`"ALL_ATTRIBUTES"`, `"ALL_PROJECTED_ATTRIBUTES"`, `"SPECIFIC_ATTRIBUTES"` or `"COUNT"`, optional)
  - See the docs ([Select for Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Select) or [Select for Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select)) for more details.

#### Options for **scan**

- **segment**: A segment to be scanned by a particular worker. Each worker should use a different value for **segment**. If **segment** is not specified and **total_segment** is specified, this plugin automatically set **segment** following the number of embulk workers. If **segment** and **total_segment** is specified, this plugin loads only the **segment**, so you loads other segments in other processes. (int, optional)
  - See the doc ([Parallel Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)) for more details.
- **total_segment**: The total number of segments for the parallel scan. If **segment** is not specified and **total_segment** is specified, this plugin automatically set **segment** following the number of embulk workers.
  - See the doc ([Parallel Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)) for more details.

#### Options for **query**

- **key_condition_expression**: To specify the search criteria, you use a key condition expression—a string that determines the items to be read from the table or index. (string, required)
  - See the doc ([Key Condition Expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions)) for more details.
- **scan_index_forward**: By default, the sort order is ascending. To reverse the order, set this option is `false`. (boolean, optional, default: `false`)
  - See the doc ([Key Condition Expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions)) for more details

#### About `DynamodbAttributeValue` Type

This type of `DynamodbAttributeValue` is one that can express Dynamodb `AttributeValue` as Embulk configuration. This configuration has the below options. Only one of these options can be set.

- **S**: string value (string, optional)
- **N**: number value. (string, optional)
- **B**: binary value. (string, optional)
- **SS**: array of string value. (array of string, optional)
- **NS**: array of number value. (array of number, optional)
- **BS**: array of binary value. (array of binary, optional)
- **M**: map value. (string to `DynamodbAttributeValue` map, optional)
- **L**: list value. (array of `DynamodbAttributeValue`, optional)
- **NULL**: null or not. (boolean, optional)
- **BOOL**: `true` or `false`. (boolean, optional)

## Example

- Scan Operation

```yaml
in:
  type: dynamodb
  auth_method: env
  region: us-east-1
  scan:
    total_segment: 20
  table: embulk-input-dynamodb_example
  columns:
    - {name: ColumnA, type: long}
    - {name: ColumnB, type: double}
    - {name: ColumnC, type: string, attribute_type: S}
    - {name: ColumnD, type: boolean}
    - {name: ColumnE, type: timestamp}
    - {name: ColumnF, type: json}

out:
  type: stdout
```

- Query Operation

```yaml
in:
  type: dynamodb
  auth_method: env
  region: us-east-1
  query:
    key_condition_expression: "#x = :v"
    expression_attribute_names:
      "#x": primary-key
    expression_attribute_values:
      ":v": {S: key-1}
  table: embulk-input-dynamodb_example

out:
  type: stdout
```

You can see more examples [here](./example).

## Development

### Run examples

```shell
$ ./run_dynamodb_local.sh
# Set dummy credentials (access_key_id=dummy and secret_access_key=dummy)
$ aws configure
$ ./example/prepare_dynamodb_table.sh
$ ./gradlew gem
$ embulk run example/config-query.yml -Ibuild/gemContents/lib
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

[MIT LICENSE](./LICENSE.txt)
