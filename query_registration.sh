queryFile=$1
libPath="./:./lib/*:./lib/jena/*:./lib/neo4j/*"
javac -cp $libPath main/QueryRegistryEngine.java 
javac -cp $libPath main/QueryRegistryEngine $queryFile
