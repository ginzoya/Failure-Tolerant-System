Team FoxHound
Aaron Lo
Jeremy Lo
Kevin Chung
Geoff Dirk
Assigntment 4: Failure-Tolerant System.

Instructions:

Run the "runsystem.sh" application in a terminal with the following parameters:
1. The Zookeeper server running address. Example: cloudsmall1.cs.surrey.sfu.ca/<some-unique-string-here>
2. The In_queue name. Example: in_queue
3. The Out_queue name. Example: out_queue
4. The write capacity for the DB instances. Example: 5
5. The read capacity for the DB instances. Example: 5
6. The names of the Database instances (seperated by commas). Example: DB1,DB2,DB3
7. The names of the Database proxies (seperated by commas)(can be blank). Example: ""
8. The base port address. Example: 7777

A successful command would look something like:
<path-to-set_aws_env_keys> ./runsystem.sh cloudsmall1.cs.surrey.sfu.ca/fox_hound in_queue out_queue 5 5 DB1,DB2,DB3 "" 7777
