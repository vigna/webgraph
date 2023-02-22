#!/bin/bash -e

if [[ "$2" == "" ]]; then
	echo "$(basename $0) DIR NTHREADS [OUTPUT]" 1>&2
	echo "Reads files in DIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Files are processed as input files unless OUTPUT is specified." 1>&2
	exit 1
fi

DIR=$1
NTHREADS=$2
OUTPUT=$3

FILES=$(mktemp)
find $DIR -type f >$FILES

NFILES=$(cat $FILES | wc -l)

if (( NFILES < 2 * NTHREADS )); then
	echo "$NTHREADS threads > $NFILES files" 1>&2
	exit 1
fi

SPLIT=$(mktemp)
split -n l/$NTHREADS $FILES $SPLIT
SPLITS=$(for file in ${SPLIT}?*; do echo $file; done)

for file in $SPLITS; do 
	mkfifo $file.pipe
	if [[ "$OUTPUT" != "" ]] ;
		(cut -f2,7,10 $(cat $file) | awk '{ if ($3 != 0) print $2 "\t" $1 }' | sort -k2 -S2G >$file.pipe) &
	else
		(cut -f7,13 $(cat $file) | sort -k2 -S2G >$file.pipe) &
	fi
done

sort -S2G -m $(for file in $SPLITS; do echo $file.pipe; done)

rm $FILES
rm ${SPLIT}*