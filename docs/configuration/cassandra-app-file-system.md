---
layout: default
---
(cassandra)=
# Cassandra file system

The `cassandra-app-file-system` module is used by [AFS](../data/storage) to define a remote Cassandra file system.

## Required properties

**drive-name**  
The `drive-name` property is a required property that defines the primary drive's name.

**ip-addresses**  
The `ip-addresses` property is a required property that defines the primary drive's IP addresses.

## Optional properties

**drive-name-X**  
The `drive-name-X` property is an optional property that defines the Xth secondary drive's name.

**ip-addresses-X**  
The `ip-addresses-X` property is an optional property that defines the Xth secondary drive's IP addresses.

**local-dc**  
The `local-dc` property is an optional property that defines the name of the datacenter for the primary drive. The default value of this property is `null`.

**local-dc-X**  
The `local-dc-X` property is an optional property that defines the name of the datacenter for the Xth secondary drive. The default value of this property is `null`.

**remotely-accessible**  
The `remotely-accessible` property is an optional property that defines whether the primary drive is remotely accessible or not. The default value of this property is `false`.

**remotely-accessible-X**  
The `remotely-accessible-X` property is an optional property that defines whether the Xth secondary drive is remotely accessible or not. The default value of this property is `false`.

## Examples

**YAML configuration:**
```yaml
cassandra-app-file-system:
  drive-name: drive1
  ip-addresses:
    - 127.0.0.1
    - <other IP>
  local-dc: dc1
  remotely-accessible: true
```
