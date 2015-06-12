#Elastic Search - Hadoop Query API
Perform queries specifying the terms in Elastic Search filtered by a timestamp range on the URL.
Output file is a set of JSON documents.

## How to run

Compile and package the code using

    mvn package

Now you should have a .jar called `elastic-hadoop-0.0.1-SNAPSHOT-job.jar`
in the directory `target` that contains all the classes necessary to run. Copy
the .jar to the cluster and execute using 

    hadoop jar elastic-hadoop-0.0.1-SNAPSHOT-job.jar outputJsonFile arguments

where the following arguments has to be used:

| argument     | description                                     |
|--------------|-------------------------------------------------|
| -query terms | The terms you want to find in URLs              |
| -DateFrom    | The initial timestamp to match the query        |
| -DateTo      | The final timestamp                             |


e.g.

    hadoop jar elastic-hadoop-0.0.1-SNAPSHOT-job.jar /user/souza/json/test.json "\"fussball wm\"" 20060101022544 20061231022814

## Output format

The Output file contains JSON records per each line and each JSON document with
the following fields:

| field | description                                          |
|-------|------------------------------------------------------|
|ts     | timestamp matched in the query 											 |
|id     | ts + original url                                    |
|comp   | compressedsize                                       |
|redir  | Redirect URL                                         |
|file   | Filename                                             |
|offset | -                                                    |
|orig   | The URL which contains query terms                   |
