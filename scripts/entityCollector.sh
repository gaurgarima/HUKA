datafile=$1
#echo $datafile
cut -f1 $datafile >> "entityFile.txt"
cut -f3 $datafile >> "entityFile.txt"
sort entityFile.txt | uniq > tmp
awk '{print NR"\t"$0}' tmp > entityFile.txt
rm tmp
#mv entityFile.txt ./data/
