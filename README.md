# HUKA (Maintaining How provenance under Updates to Knowledge Graphs)

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

 * We also need 'google.guava.XX' java APIs (.jar).

You need to download and move the jar files in appropriate duectories as mentioned along side.
You can also download all the java API using link (host all used apisXX). 

##Usage

HUKA performs three main task (in order) -- creating and populating databases, registering queries and then finally, handling KG update requests.
We next provide details of how to perform each task, along with their input file format.


1. `Database construction`: Run the bash script `XX` in scripts/ directory. It expects a file containing list of all the triples consisting a dataset as input. 
```
cd scripts
./XX <factFile>
```

2. `Query Registration`: A bash script `query_registration.sh` compiles and runs query registration module which build all required supporting 
data structures.
```
./XX <queryFile>
```

3. `Update request handling`: Run `update.sh` with a file `uspdateRequests.txt` listing all the update requests.
```
./XX <updateFile>
```


The format of expected input files is given below,

1. A tab-separated file fact file `factFile.tsv`
```
<subject1-URI>  <predicate1-URI>   <object1-URI>.
.
.
<subject-URI>  <predicate1-URI>   <object1-URI>.
```
2. `rawqueryList.txt`, a file containing list of queries which needs to be registered with HUKA for maintenance, and,
```
SPARQL-Query1
..
SPARQL-Queryn
```
3. .`updateRequest.txt` contains edge update (insertion/deletion) requests. 
```
<OutgoingVertexId>   <IncomingVertexId> <OutgoingVertexLabel>   <IncomingVertexLabel> <EdgeLable> <EdgeId> <Operation (I/D)>
```

A sample of each expected file is given in `sample/` directory.
The sample files have headers for the convenience of explaining the data, however original files do not require headers.

## License

HUKA is provided as open-source software under the MIT License. See [LICENSE](LICENSE).

## Contact

https://github.com/gaurgarima/HUKA

Garima Gaur <garimag@cse.iitk.ac.in>
