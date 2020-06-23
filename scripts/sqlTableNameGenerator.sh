# For a given list of predicates find its rdf:label triple and assign that as corresponding sql table name

queryPredFile=$1
#awk '{if $2="<http://www.w3.org/2000/01/rdf-schema#/label>", print $0}' $datafile | cut -f1,3 >  labelOnly
#}
#predicateFile="queryPredicateList.txt"
sqlTableNameFile="sqlTableName.txt"

while read -r line1
do
	a=$(basename $line1)
	a=${a//[>]}
	echo -e $line1"\t"$a >> $sqlTableNameFile
#	rev $line1 | cut -d "/" -f1 | rev > 
#	awk '{if $1=$line1,print $0}' labelOnly >> $sqlTableNameFile
done < $queryPredFile
