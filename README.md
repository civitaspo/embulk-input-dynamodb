# Dynamodb input plugin for Embulk

## Overview

* **Plugin type**: input
* **Load all or nothing**: yes
* **Resume supported**: no
* **Cleanup supported**: no


## Configuration
- **access_key**: AWS access key (string, optional)
- **secret_key**: AWS secret key (string, optional)  
If you don't specify keys, I'll use the profile configuration file for the default profile.
- **region**: Region Name (string, default: ap-northeast-1)
- **table**: Table Name (string, required)
- **scan_limit**: DynamoDB 1time Scan Query size limit (Int, default: 100) 
- **record_limit**: Max Record Search limit (Long, default: 100000) 

## Example

```yaml
in:
  type: dynamodb
  access_key: YOUR_ACCESS_KEY
  secret_key: YOUR_SECRET_KEY
  region: ap-northeast-1
  table: YOUR_TABLE_NAME
  columns:
    - {name: ColumnA, type: long}
    - {name: ColumnB, type: double}
    - {name: ColumnC, type: string}
    - {name: ColumnD, type: boolean}
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
