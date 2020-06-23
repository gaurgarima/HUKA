cut -f2 $1 |sort| uniq > tmp

#head -5 tmp

input="tmp"
while IFS= read -r line
do
#	echo $line
  	a=$(basename $line)
	a=${a//[>]}
	echo -e $line"\t"$a >> "relationFile.txt"

done < "$input"

rm tmp
#mv relationFile.txt ./data/
