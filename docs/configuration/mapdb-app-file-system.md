---
layout: default
todo:
    - add link to AFS concept
    - add link to MapDB AFS implementation
---

# Mapdb file system
The `mapdb-app-file-system` module is used by [AFS](../data/storage) to define one or several drives mapped to a [MapDB](http://www.mapdb.org) file.

## Required properties

**db-file**  
The `db-file` property is a required property that defines the path to the primary MapDB drive. If the file does not exist, it is automatically created at startup.

**drive-name**  
The `drive-name` property is a required property that defines the primary drive's name.

## Optional properties

**db-file-X**  
The `db-file-X` property is an optional property that defines the Xth secondary MapDB drive. This parameter is required if the `drive-name-X` property is set.

**drive-name-X**  
The `drive-name-X` property is an optional property that defines the Xth drive's name. This parameter is required if the `db-file-X` property is set.

**max-additional-drive-count**  
The `max-additional-drive-count` property is an optional property that defines the maximal number of secondary drives.

**remotely-accessible**  
The `remotely-accessible` property is an optional property that defines whether the primary drive is remotely accessible or not. The default value of this property is `false`.

**remotely-accessible-X**  
The `remotely-accessible-X` property is an optional property that defines whether the Xth secondary drive is remotely accessible or not. The default value of this property is `false`.

## Examples

**YAML configuration:**
```yaml
mapdb-app-file-system:
    max-additional-drive-count: 2
    drive-name: drive1
    db-file: /home/user/drive1.db
    remotely-accessible: true
    drive-name-0: drive2
    db-file-0: /home/user/drive2.db
    drive-name-1: drive3
    db-file-1: /home/user/drive3.db
    remotely-accessible-1: true
```

**XML configuration:**
```xml
<mapdb-app-file-system>
    <max-additional-drive-count>2</max-additional-drive-count>
    <drive-name>drive1</drive-name>
    <db-file>/home/user/drive1.db</db-file>
    <remotely-accessible>true</remotely-accessible>
    
    <!-- First secondary drive -->
    <drive-name-0>drive2</drive-name-0>
    <db-file-0>/home/user/drive2.db</db-file-0>
    
    <!-- Second secondary drive -->
    <drive-name-1>drive3</drive-name-1>
    <db-file-1>/home/user/drive3.db</db-file-1>
    <remotely-accessible-1>true</remotely-accessible-1>
</mapdb-app-file-system>
```
