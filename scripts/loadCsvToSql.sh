#!/bin/bash

partitionPath=$2
dataset=$1
read -p "Enter sql username: " usrname
read -s -p "Enter password: " pswd
#echo "Hi $usrname ,  your password is $pswd"

mysql -u $usrname -p $pswd -e "create database $dataset"

for f in $partitionPath*.tsv
do 
	tableName=$(basename $f | sed 's/\.tsv//g')
	echo "Loading table $tableName"

	mysql -u root -p -e "use $dataset;select database();create table $tableName(outV text(10), inV text(10), poly varchar(20));"

#	mysql -u $usrname -p $pswd -e "create database $dataset" -e "use $dataset"  -e "create table $tableName (out TEXT, in TEXT, poly VARCHAR(20))" 

	mysql -u $usrname -p $pswd -e "use $dataset; select database(); LOAD DATA LOCAL INFILE '$f' INTO TABLE $tableName FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';";

done
