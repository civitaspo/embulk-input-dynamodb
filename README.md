# Dynamodb input plugin for Embulk

## Overview

* **Plugin type**: input
* **Load all or nothing**: yes
* **Resume supported**: no
* **Cleanup supported**: no


## Configuration

- **region**: Region Name (string, default: ap-northeast-1)
- **table**: Table Name (string, required
- **limit** Scan Limit (integer, default: 100)

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
