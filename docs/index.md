![PowSyBl Logo](_static/logos/logo_lfe_powsybl.svg)
# PowSyBl AFS

**AFS** stands for **A**pplication **F**ile**S**ystem.

An AFS is meant to be used to organize your **business** data and store them,
like a file system does for plain files.

The structure of an AFS looks like:

```
   AppData
     +-- FileSystem1
     |   +-- File1
     |   +-- File2
     |   +-- Project1
     |   |   +-- RootFolder
     |   |       +-- ProjectFile1
     |   |       +-- ProjectFolder1
     |   |       |   +-- ProjectFile2
     |   |       +-- ProjectFolder2
     |   |           +-- ProjectFile3
     |   +-- Project2
     |      ...
     |
     +-- FileSystem2
         ...
```
where each "project file" may represent a business object, for instance a network or a list of contingencies, or even a computation.


```{toctree}
---
maxdepth: 2
---

data/storage.md
configuration/index.md
itools/index.md
api/api_afs.md
```
