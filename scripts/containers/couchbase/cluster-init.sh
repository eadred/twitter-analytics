#!/bin/bash

# See https://blog.couchbase.com/using-docker-develop-couchbase/

# Check if couchbase server is up
check_db() {
 curl --silent http://127.0.0.1:8091/pools > /dev/null
 echo $?
}
	
i=1
numbered_echo() {
echo "[$i] $@"
i=`expr $i + 1`
}

set -m
/entrypoint.sh couchbase-server &

until [[ $(check_db) = 0 ]]; do
>&2 numbered_echo "Waiting for Couchbase Server to be available"
sleep 1
done
 
echo "# Couchbase Server Online"
echo "# Starting setup process"
couchbase-cli cluster-init -c 0.0.0.0:8091 \
      --cluster-username=$USERNAME \
      --cluster-password=$PASSWORD \
      --cluster-port=8091 \
      --services=$SERVICES \
      --cluster-ramsize=$MEMORY_QUOTA \
      --wait

couchbase-cli bucket-create -c 0.0.0.0:8091 \
     --bucket=results \
     --bucket-ramsize=$MEMORY_QUOTA \
     --wait \
     -u $USERNAME -p $PASSWORD

echo "# Attaching to couchbase-server entrypoint"
fg 1