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

# To avoid empty splits, there must be at least as many threads as files

if (( NFILES < NTHREADS )); then
	NTHREADS=$NFILES
	echo "Not enough files: number of threads set to $NFILES" 1>&2
fi

SPLITBASE=$(mktemp)
split -n l/$NTHREADS $FILES $SPLITBASE
SPLITS=$(for file in ${SPLITBASE}?*; do echo $file; done)

for SPLIT in $SPLITS; do 
	mkfifo $SPLIT.pipe

	# For each file, delete first line (labels); cut will add a newline at the end if missing

	if [[ "$OUTPUT" != "" ]]; then
		( while read FILE; do
			cut -f2,7,10 "$FILE" | tail -n+2 | awk '{ if ($3 == 0) print $1 "\t" $2 }'
		done <$SPLIT | LC_ALL=C sort -S2G >$SPLIT.pipe) &
	else
		( while read FILE; do
			cut -f7,13 "$FILE" | tail -n+2 | awk '{ print $2 "\t" $1 }'
		done <$SPLIT | LC_ALL=C sort -S2G >$SPLIT.pipe) &
	fi
done

LC_ALL=C sort -S2G -m $(for SPLIT in $SPLITS; do echo $SPLIT.pipe; done)

rm -f $FILES
rm -f ${SPLITBASE}*
