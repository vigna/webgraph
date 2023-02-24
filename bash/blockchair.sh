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

SPLIT=$(mktemp)
split -n l/$NTHREADS $FILES $SPLIT
SPLITS=$(for file in ${SPLIT}?*; do echo $file; done)

for file in $SPLITS; do 
	mkfifo $file.pipe
	if [[ "$OUTPUT" != "" ]]; then
		(tail -q -n+2 $(cat $file) | cut -f2,7,10 | awk '{ if ($3 == 0) print $2 "\t" $1 }' | LC_ALL=C sort -k2 -S2G >$file.pipe) &
	else
		(tail -q -n+2 $(cat $file) | cut -f7,13 | LC_ALL=C sort -k2 -S2G >$file.pipe) &
	fi
done

LC_ALL=C sort -k2 -S2G -m $(for file in $SPLITS; do echo $file.pipe; done)

rm -f $FILES
rm -f ${SPLIT}*
