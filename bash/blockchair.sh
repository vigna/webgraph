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

function file_ends_with_newline() {
	[[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

FILES=$(mktemp)
find $DIR -type f >$FILES

# Check that all files end with a newline

while read FILE; do
	if ! file_ends_with_newline $FILE; then
		echo "File $FILE does not end with a newline" 1>&2
		exit 1
	fi
done <$FILES

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
	if [[ "$OUTPUT" != "" ]]; then
		(tail -q -n+2 $(cat $SPLIT) | cut -f2,7,10 | awk '{ if ($3 == 0) print $1 "\t" $2 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
	else
		(tail -q -n+2 $(cat $SPLIT) | cut -f7,13 | awk '{ print $2 "\t" $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
	fi
done

LC_ALL=C sort -S2G -m $(for SPLIT in $SPLITS; do echo $SPLIT.pipe; done)

rm -f $FILES
rm -f ${SPLITBASE}*
