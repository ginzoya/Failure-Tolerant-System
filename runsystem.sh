#!/bin/bash
# This script initalizes frontend, backend, and the 3 databases.
# A string identifying the ZooKeeper host and its port number, to be passed directly to the constructor for KazooClient. You should not try to interpret this string, but simply pass it directly to the constructor.
# The name of the SQS-in queue. A string of characters and hyphens with no whitespace or other punctuation.
# The name of the SQS-out queue. A string of characters and hyphens with no whitespace or other punctuation.
# The provisioned write capacity for the DynamoDB table created by each database instance. Integer, writes/s.
# The provisioned read capacity for the DynamoDB table created by each database instance. Integer, reads/s.
# A list of instance names for your database instances.
# A list of names of database instances that will be proxied for publish/subscribe.
# A base port address.

ZK_string=$1
In_queue=$2
Out_queue=$3
Write_capacity=$4
Read_capacity=$5
DB_names=$6
DB_proxy=$7
Base_port=$8

if [ "$#" != "8" ]; then
	DB_proxy='NONE'
	Base_port=$7
fi

# Pass ZK_string to DBs
# Pass in_queue to Frontend, DBs
# Pass out_queue to Backend, DBs
# Pass Write/Read Capacity to DBs

if [ "$1" == "-h" ]; then
	printf "This script initalizes frontend, backend, and the 3 databases. The following params are required:\n
1.A string identifying the ZooKeeper host and its port number, to be passed directly to the constructor for KazooClient. You should not try to interpret this string, but simply pass it directly to the constructor.
2.The name of the SQS-in queue. A string of characters and hyphens with no whitespace or other punctuation.\n
3.The name of the SQS-out queue. A string of characters and hyphens with no whitespace or other punctuation.\n
4.The provisioned write capacity for the DynamoDB table created by each database instance. Integer, writes/s.\n
5.The provisioned read capacity for the DynamoDB table created by each database instance. Integer, reads/s.\n
6.A list of instance names for your database instances.\n
7.A list of names of database instances that will be proxied for publish/subscribe.\n
8.A base port address."
	exit 0
fi

# Starts frontend
#python frontend.py $In_queue &

# Starts backend
#python backend.py $Out_queue &

DB_list=""
IFS=$","
for name in $DB_names; do
	DB_list="$DB_list $name"
done
unset IFS

# Starts the DBs
for DB_name in $DB_list; do
	python dbinstance.py $ZK_string $In_queue $Out_queue $Write_capacity $Read_capacity $DB_name $DB_names $DB_proxy $Base_port && fg
	#echo $DB_name
done

exit 0
