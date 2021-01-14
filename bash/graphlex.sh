#!/bin/bash -e

if [[ "$@" == "" ]]; then
	echo "USAGE: $(basename $0) PERM" 1>&2
	echo "Reads a list of IDs from standard input, writes on standard output the IDs sorted with the C locale, and saves in PERM the sorting permutation. The permutation can be used directly with Transform to permute a graph by lexicographical ID order." 1>&2
	exit 1
fi

nl -v0 -nln | \
	LC_ALL=C sort -S2G -T. -k2 | \
	tee >(
			cut -f1 | \
			tr -d ' ' | \
			nl -v0 -nln | \
			LC_ALL=C sort -S2G -T. -n -k2 | \
			cut -f1 | \
			java it.unimi.dsi.law.io.tool.Text2DataOutput -t int - $1
	) | \
	cut -f2
