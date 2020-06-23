
queryFile=$1
libPath="./:./lib/*:./lib/jena/*:./lib/neo4j/*:./lib/virtuoso-jena/*"
javac -cp $libPath main/QueryRegistryEngine.java 
# javac -cp $libPath main/QueryRegistryEngine $queryFile
