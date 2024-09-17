---
layout: default
---
(cassandra)=
# Cassandra storage

The `cassandra-app-storage` module is used by [AFS](../data/storage) to define some properties of the access to the Cassandra database.

## Required properties

No property is required, all are optional.

## Optional properties

**flush-maximum-change**  
The `flush-maximum-change` property is a required property that defines buffer maximum change. The default value of this property is `1000`.

**flush-maximum-size**  
The `flush-maximum-size` property is a required property that defines buffer maximum size. The default value of this property is `2^20`.

**double-query-partition-size**  
The `double-query-partition-size` property is an optional property that defines the maximum number of DoubleTimeSeries queried at once. The default value of this property is `1000`.

**string-query-partition-size**  
The `string-query-partition-size` property is an optional property that defines the maximum number of StringTimeSeries queried at once. The default value of this property is `1000`.

**binary-data-chunk-size**  
The `binary-data-chunk-size` property is an optional property that defines the buffer capacity in bytes. The default value of this property is `2^20`.

## Examples

**YAML configuration:**
```yaml
cassandra-app-storage:
  flush-maximum-change: 100
  double-query-partition-size: 100
  string-query-partition-size: 100
```
