---
layout: default

todo:
  - Add links to powsybl-core documentation when available (#groovy-extensions)
  - Add missing links 
---

# iTools run-script

The `run-script` command is used to run scripts based on PowSyBl.

See [PowSyBl Core documentation](http://powsybl-core.readthedocs.io/) for more information 
about general usage.
 
## Usage
```
$> itools run-script --help
usage: itools [OPTIONS] run-script --file <FILE> [--help]

Available options are:
    --config-name <CONFIG_NAME>   Override configuration file name

Available arguments are:
    --file <FILE>   the script file
    --help          display the help and quit
```

### Required arguments

`--file`: This option defines the path of the script to execute. Current, only Groovy scripts are supported.

## Groovy extensions
The `run-script` command relies on a [plugin mechanism]() to load extensions. Those extensions 
provide utility functions to make the usage of PowSyBl easier through the scripts. It avoids 
the user to write boilerplate code hiding the technical complexity of framework into more user 
friendly functions. PowSyBl provides the following extensions to:
- [load a network from a file](https://powsybl-core.readthedocs.io/en/latest/itools/run-script.md#load-a-network)
- [save a network to a file](https://powsybl-core.readthedocs.io/en/latest/itools/run-script.md#save-a-network)
- [run a power flow simulation](https://powsybl-core.readthedocs.io/en/latest/itools/run-script.md#run-a-power-flow)
- [access to AFS](#access-to-afs)

### Access to AFS
The `Afs` extension adds a `afs` variable to the groovy binding that offers a facade to access data stored in [AFS](../index.md). This facade has two methods:
- `getFileSystemNames`: this method returns the names of the file system declared in the configuration
- `getRootFolder`: this method returns the root [folder]() of the specified file system. From this root folder, it is possible to navigate in the different folders and open the different projects. 

In order to benefit from this feature, add `com.powsybl:powsybl-afs-scripting` to your classpath.

**Example**
```groovy
fileSystems = afs.getFileSystemNames()
for (String fs : fileSystems) {
    root = afs.getRootFolder(fs)
}
```

## Examples

### Example 1 - Hello World
The following example shows how to run a simple HelloWorld script. Note that the parameters pass to the command line can be accessed using the `args` array. 

**Content of the hello.groovy file:**
```groovy
print 'Hello ' + args[0]
```

To run this script, pass this file to the `--file` argument:
```
$> itools run-script hello.groovy John
Hello John
```

### Example 2 - TODO

TODO

## Going further
- [Create a Groovy extension](): Learn how to create a groovy extension to use it with the `run-script` command
