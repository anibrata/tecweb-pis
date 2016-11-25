#!/bin/bash

ipf='skspeare.txt'

if [ -s "$ipf" ]
then
	rm "$ipf"
else
	touch "$ipf"
fi

filename="Shakespeare.txt"

while IFS='' read -r line || [[ -n "$line" ]]; do
	#temp=`echo $line | tr '\n' ' ' | sed 's/^ *//g'`
	if [ "$line" != "THE END" ]
	then
		echo $line|tr '\n' ' '|perl -i -ne 's/^\s+//;print' >> "$ipf"
	else
		echo $line >> "$ipf"
	fi	
done < $filename

