
changesFile=$1
libPath="./:./lib/*:./lib/jena/*:./lib/neo4j/*:./lib/virtuoso-jena/*"

javac -cp $libPath main/Main.java
#java -cp $libPath main/Main $changesFile
