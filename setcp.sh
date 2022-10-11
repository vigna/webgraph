#!/bin/sh

sourcedir=$(cd -- "$(dirname "$0")" && pwd)
webgraphjars=$(find "$sourcedir" -maxdepth 1 -name "*.jar" | grep "webgraph-")
count=$(echo "$webgraphjars" | wc -l)

if [ "$count" -eq 0 ]; then
	echo "WARNING: no webgraph jar file."
elif [ "$count" -gt 1 ]; then
	echo "WARNING: several webgraph jar files: $webgraphjars"
else
	if echo "$CLASSPATH" | grep -q slf4j; then
		deps=$(find "$sourcedir"/jars/runtime -name "*.jar" | grep -v slf4j | paste -d: -s -)
	else
		deps=$(find "$sourcedir"/jars/runtime -name "*.jar" | paste -d: -s -)
	fi

	CLASSPATH=$webgraphjars:$deps:$CLASSPATH
	export CLASSPATH
fi
