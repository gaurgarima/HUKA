
changesFile=$1
dataset=$2
libPath="./:./lib/*:./lib/jena/*:./lib/neo4j/*"
javac -cp $libPath main/Main.java
java -cp $libPath main/Main $changesFile
