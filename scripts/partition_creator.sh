
#!/bin/bash

#IFS=' '

#declare -A fileMap
#datafile="latest-all.nt"
datafile=$1
echo "$datafile $datafile"
while read -r line1
do
	read -ra ADDR <<< "$line1"
#	fileMap[${ADDR[0]}]="${ADDR[1]}"

#	echo "${ADDR[0]}"
#	echo "${fileMap[${ADDR[0]}]}"
#	fgrep -n "${ADDR[0]}" $datafile	| sed 's/:/ /' >> "partition/${ADDR[1]}"
#	awk '{print $2}' $datafile
	a=${ADDR[0]}
	awk -F '\t'  -v relation="${ADDR[0]}" '{if ($2==relation) print $1,"\t",$3,"\t",$4;}' $datafile >> "partition/${ADDR[1]}"

	
done < "sqlTableName.txt"

