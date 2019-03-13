# Infinispan Directory Copy Example

This example will copy the file contents of some input directory (not recursively) into the Infinispan-based block store, then out again to a destination directory. The result SHOULD be that the input and output directories are the same.

## Build

```
	$ mvn clean package
```

## Usage

```
	$ java -jar target/ispn-dir-copy-ex-2.0-SNAPSHOT-jar-with-dependencies.jar <input-dir> <output-dir>
```
