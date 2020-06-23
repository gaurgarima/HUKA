
datafile=$1
grep --binary-file=text '[.]$' $1| awk -F " " 'NF>4{print}' | rev | cut -d " " -f3- | rev > test
sed -i  -e 's/ /\'$'\t/' -e 's/ /\'$'\t/' test
sed -i 's_://_:_g' test
awk  '{if (($1 ~ /^</) && ($1 ~ />$/) && ($2 ~ /^</) && ($2 ~ />$/)) print}' test >> "factFile.txt"
rm  test  
