
datafile=$1

awk '{print $0"\t"NR}' $datafile > tmp
mv tmp $datafile
