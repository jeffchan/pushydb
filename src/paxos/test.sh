#!/bin/bash

trap "echo Exited!; exit;" SIGINT SIGTERM

for i in $(seq 0 20);
do
	 go test $1 2>/dev/null | egrep -v 'EOF|files|connection|broken|connected' > $i.log &
done
