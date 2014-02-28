#!/bin/bash

trap "echo Exited!; exit;" SIGINT SIGTERM

for i in $(seq 0 20);
do
	 go test $1 2>${i}a.error | egrep -v 'EOF|files|connection|broken|connected|wrong' > ${i}a.log
	 go test $1 2>${i}b.error | egrep -v 'EOF|files|connection|broken|connected|wrong' > ${i}b.log
done
