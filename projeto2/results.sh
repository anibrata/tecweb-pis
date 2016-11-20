#!/bin/sh

echo "Data for Shakespeare's Combined works text file"
echo "------------------------------------------------"
echo "------------------------------------------------"
echo ""
echo ""
cd dataPairs
echo "Results for the pairs data"
echo "--------------------------"
var=`wc -l part-r-00000-pairs-sk | awk '{print $1}'`
echo "Number of distinct pairs :$var"
echo ""
echo ""
vstring=`sort -r -nk3 part-r-00000-pairs-sk | awk '{print $1, $2}' | head -1`
echo "Pair with highest PMI"
echo "---------------------"
echo "$vstring"
echo ""
echo ""
r1=`grep "life" part-r-00000-pairs-sk  | sort -r -nk3 | head -3`
echo "Terms with highest PMI with the word life"
echo "------------------------------------------"
echo "$r1"
echo ""
echo ""
r2=`grep "love" part-r-00000-pairs-sk  | sort -r -nk3 | head -3`
echo "Terms with highest PMI with the word love"
echo "------------------------------------------"
echo "$r2"
echo ""
echo ""
echo "Data for Simple Wiki text file"
echo "------------------------------------------------"
echo "------------------------------------------------"
echo ""
echo ""
cd ../dataPairs
echo "Results for the pairs data"
echo "--------------------------"
var=`wc -l part-r-00000-pairs-sw | awk '{print $1}'`
echo "Number of distinct pairs :$var"
echo ""
echo ""
vstring=`sort -r -nk3 part-r-00000-pairs-sw | awk '{print $1, $2}' | head -1`
echo "Pair with highest PMI"
echo "---------------------"
echo "$vstring"
echo ""
echo ""
r1=`grep "life" part-r-00000-pairs-sw  | sort -r -nk3 | head -3`
echo "Terms with highest PMI with the word life"
echo "------------------------------------------"
echo "$r1"
echo ""
echo ""
r2=`grep "love" part-r-00000-pairs-sw  | sort -r -nk3 | head -3`
echo "Terms with highest PMI with the word love"
echo "------------------------------------------"
echo "$r2"
echo ""
