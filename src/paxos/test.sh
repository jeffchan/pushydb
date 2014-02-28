for i in $(seq 1 20);
do
	 go test 2>/dev/null | egrep -v 'EOF|connection|broken|connected'
done