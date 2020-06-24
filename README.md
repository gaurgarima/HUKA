# HUKA

HUKA is a framework which maintains result of graph queries along  with their provenance under dynamic knowledge graphs (KG).
Addition or removal of new information in a knowledge graph often leads to insertion or deletion of an edge in the KG.
HUKA efficiently identifies and update precomputed answers of all the registered queries which would be affected by a given change in the KG.
HUKA currently supports positive conjunctive SPARQL queries. The provenance model employed is an
adaptation of the popular how-provenance model, provenance semiring. 

## Prerequisites for installation

 * An installation of `Neo4j` graph database along with bulk-import utility. The framework currently uses
XX version. You can download Neo4j from their official download page (URL).
Unzip the folder and rename it to neo4j and place it in lib directory of the file

 * Setup `MariaDB` XX. Along with jdbc pacakge () to access the SQL db. 

 * `Jena` XX, a freely available XX system, can be downloaded from (). We require java apis which can be found in lib
dir8ectory. 

 * We also need com.google.guava_1.6.0.jar java APIs (.jar).

You need to download and move the jar files in appropriate duectories as mentioned along side.
You can also download all the java API using link (host all used apisXX). 

## Usage

### Input File Format

The format of 3 files which user needs to supply to the framework are as following,

1. A tab-separated file *factFile.tsv* listing all the facts of the knowledge graph as triples.
```
<subject1-URI>  <predicate1-URI>   <object1-URI>
.
.
<subject-URI>  <predicate1-URI>   <object1-URI>
```

2. A file, *rawQueryList.txt*, containing all the sparql queries which user wants to register with HUKA for maintenance. Each line should contain a single query as shown below,
```
SPARQL-Query1
..
SPARQL-Queryn
```

3. Lastly, a tab-separated file *updateRequest.txt* listing down all the edge update (insertion/deletion) requests in the following format,
```
<OutgoingVertexId_i>   <IncomingVertexId_i> <OutgoingVertexLabel_i>   <IncomingVertexLabel_i> <EdgeLabel_i> <EdgeId_i> <Operation_i (I/D)>
.
<OutgoingVertexId_j>   <IncomingVertexId_j> <OutgoingVertexLabel_j>   <IncomingVertexLabel_j> <EdgeLabel_j> <EdgeId_j> <Operation_j (I/D)>
```

A sample of each expected file is given in [sample/](sample/) directory. These sample files have headers for the convenience of explaining the data, however original files do not require headers.

### Framework

HUKA performs three main task (in order) -- creating and populating databases, registering queries and then finally, handling KG update requests.
We next provide details of how to perform each task, along with their input file format.


1. `Database construction`: Run the bash script [prepareDataFile.sh](test/sql/prepareDataFile.sh) in directory [scripts](scripts/). It works with the file containing list of all the triples consisting a dataset.
```
cd scripts
./prepareDataFile.sh <factFile> <datasetName>
```
After execution of  [prepareDataFile.sh](test/sql/prepareDataFile.sh), the fact file could be found in directory [/meta/dataset/kg/raw/]([/meta/dataset/kg/raw/). Before, next two tasks, query registration and maintaining query results, set few parameters in [conf](conf) file. The parameter values which a user needs to set are marked with * in [conf](conf) file.

2. `Query Registration`: A bash script [query_registration.sh](query_registration.sh) compiles and runs query registration module which build all required supporting data structures.
```
./query_registration.sh <queryFile> <datasetName>
```

3. `Update request handling`: Run [update.sh](update.sh) with the *updateRequests.txt* file listing all the update requests.
```
./update.sh <updateFile> <datasetName>
```

## License

HUKA is provided as open-source software under the MIT License. See [LICENSE](LICENSE).

## Contact

https://github.com/gaurgarima/HUKA

Garima Gaur <garimag@cse.iitk.ac.in>
