#!/bin/bash
# Usage: ./test.sh [-test.run TestUnreliable]

trap "echo Exited!; exit;" SIGINT SIGTERM

mkdir -p out

for i in $(seq 0 1000);
do
  echo "Running trial ${i}"
	go test $1 2>out/${i}.error | egrep -v 'EOF|files|connection|broken|connected' > out/${i}.log
done
