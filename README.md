# About...

This project creates Inverted Index from a collection of documents

# Assumption...

The following assumption has been made for the project,
1. Characters in set `[a-z][A-Z][0-9]` are considered as valid characters of a word. Any other characters (Example: `;:- _`  etc...) are considered as delimiters (so never part of the word)  
 
# How to compile?

#### To cleanup the build...

```
sbt clean;
```

#### To build fat jar...
```
sbt assembly;
```

# How to execute?

```
spark-submit --master <yarn/local[*]> \
--class com.cf.revindex.RevIndexer \
<Path/To/>rev-index-assembly-0.1.0-SNAPSHOT.jar \
--sourcePath <Path of the document files> \
--targetPath <Path to write output> \
--documentDictionaryPath <OPTIONAL: Path to store document Dictionary> \
--wordDictionaryPath <OPTIONAL: Path to store word Dictionary>
```

# Code Links...

Program starts [here](src/main/scala/com/cf/revindex/RevIndexer.scala)

All the transformations are available [here](src/main/scala/com/cf/revindex/Transform.scala)
 
Test suite for the project available [here](src/test/scala/com/cf/revindex/RevIndexerTest.scala)

Resource for the test suite available [here](src/test/resources/sample1)
