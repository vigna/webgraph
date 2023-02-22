#!/bin/bash -e

if [[ "$2" == "" ]]; then
	echo "$(basename $0) DIR NTHREADS [OUTPUT]" 1>&2
	echo "Reads files in DIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Files are processed as input files unless OUTPUT is specified." 1>&2
	echo "FILES MUST END WITH A NEWLINE. Fix them with \"sed -i -e '\$a\\' *\"." 1>&2
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
	if [[ "$OUTPUT" != "" ]]; then
		(tail -q -n+2 $(cat $file) | cut -f2,7,10 | awk '{ if ($3 == 0) print $2 "\t" $1 }' | sort -k2 -S2G >$file.pipe) &
	else
		(tail -q -n+2 $(cat $file) | cut -f7,13 | sort -k2 -S2G >$file.pipe) &
	fi
done

sort -k2 -S2G -m $(for file in $SPLITS; do echo $file.pipe; done)

rm $FILES
rm ${SPLIT}*
