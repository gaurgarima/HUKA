
# First create entity, relation files, then assign annotate data and create Neo4J import files

datafile=$1
echo -e "\n********************************* Starting processing ***********************************************"

echo -e "\nConstructing list of entites invovled in the dataset (stored in file entityFile.txt along with their id)"
source ./entityCollector.sh $datafile
echo "Total $(wc -l entityFile.txt| cut -d ' ' -f1) unique entities"
echo -e "\n========================================================================================================"

echo -e "\nMaking a list of predicates involved in dataset along with their labels (stored in file relationFile.txt)"
source ./relationCollector.sh  $datafile
echo "Total $(wc -l relationFile.txt| cut -d ' ' -f1) unique predicates"
echo -e "\n========================================================================================================"

echo "\nAnnotating facts by assigning a ID to each fact"
source ./assignProvPoly.sh $datafile
echo "Total $(wc -l $1) facts"
echo "======================================================================================================="

echo -e "\nPreparing data to bulk load to Neo4J"
javac Neo4JImport.java
java Neo4JImport
echo -e "\n===================================================================================================="

# First find predicates invovled in queries
echo -e "\nExtracting predicateds invovled in the queries"
javac -cp "./lib/apache-jena-1.1/lib/*" QueryParser.java
java -cp ".:./lib/apache-jena-1.1/lib/*" QueryParser
echo -e "Total $(wc -l queryPredicateList.txt) query predicates"
source ./sqlTableNameGenerator queryPredicateList.txt
echo -e "\n===================================================================================================="

#echo -e "\nCreating partitions based on query predicates invovled"
mkdir partition
source ./partition_creator $datafile


#Now create mysql tables and populate data
echo -e "\nLoading data to SQL tables"
source ./loadCsvToSql  
echo "=========================================================================================================="

#Write Neo4j bulk import command

echo -e "\nsetting up directory structure"
mkdir ../data
mkdir ../query
mkdir ../ experiment
mkdir ../experiment/workload/
mkdir ../experiment/runtime/

mv entityFile.txt ./data/
mv relationFile.txt ./data/
mv factFile.txt ./data/
mv factsWithId.txt ./data/
mv sqlTableName.txt ./data/
mv partition ./data/
mv rawQueryList.txt ./query/
mv queryList.txt ./query/
