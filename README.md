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
- **limit**: Scan Limit (integer, default: 100)

## Example

```yaml
in:
  type: dynamodb
  region: ap-northeast-1
  table: YOUR_TABLE_NAME
  limit: 1000
  columns:
    - {name: ColumnA, type: long}
    - {name: ColumnB, type: double}
    - {name: ColumnC, type: string}
    - {name: ColumnD, type: boolean}
```

## Build

```
$ ./gradlew gem
```
