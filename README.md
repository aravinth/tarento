# Tarento Exercise 
===================

Let’s assume that there are N number of categories and under each category there are M number of English sentences. 
Also assume that your system receives S number of sample sentences every day. 

You have been provided (given) with algorithm A that can return similarity SM, between two sentences pair, where 0 < SM < 1. 
Your target is to build a program in Java language that can arrange incoming S number of sample under the predefined categories. 
The code should clear demonstrate following attribute
		- usage of higher order function
		- performance of the code when M, S are really large (> 1 million) and N is (around 1000).


Approach :
=========
Use Hadoop system to build scalable framework to process millions of records as per the given requirements.

The first step is to pick the best Similarity score within each category for a given input sentence by comparing against 
the other pre-existing sample sentences under each category.

The second step is to pick the best Category for each of the given input sentences.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Upload Category file into HDFS (This will go as Distributed cached file)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[aravinth@local tarento]$ cat categories.txt
animals
cars

[aravinth@local tarento]$ hadoop fs -put categories.txt /user/aravinth/


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Upload categorized input file into HDFS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[aravinth@local tarento]$ cat categorized_input.txt
animals^cats are pet
animals^tigers are wild
cars^maruthi is reliable
cars^honda is spacious

[aravinth@local tarento]$ hadoop fs -put categorized_input.txt /user/aravinth/categorized/


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Upload Actual input file into HDFS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[aravinth@local tarento]$ cat actual_input.txt
this is a cat
this is a rabbit
this is a horse
this is a mouse

[aravinth@local tarento]$ hadoop fs -put actual_input.txt /user/aravinth/input/



~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Kick to pick the highest Similarity Score for a given sentence with each category.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[aravinth@local ~]$ hadoop jar /tmp/category-mr-1.0-jar-with-dependencies.jar com.tarento.category.mr.SentenceCategorizer hdfs://hdfs-cluster/user/aravinth/input/actual_input.txt hdfs://hdfs-cluster/user/aravinth/categorized/categorized_input.txt hdfs://hdfs-cluster/user/aravinth/out -category hdfs://hdfs-cluster/user/aravinth/categories.txt

19/08/01 05:43:33 INFO client.AHSProxy: Connecting to Application History server at *******
19/08/01 05:43:33 INFO client.RequestHedgingRMFailoverProxyProvider: Looking for the active RM in [rm1, rm2]...
19/08/01 05:43:33 INFO client.RequestHedgingRMFailoverProxyProvider: Found active RM [rm1]
19/08/01 05:43:33 INFO input.FileInputFormat: Total input paths to process : 2
19/08/01 05:43:33 INFO mapreduce.JobSubmitter: number of splits:2
19/08/01 05:43:33 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1563260006828_1448
19/08/01 05:43:34 INFO impl.YarnClientImpl: Submitted application application_1563260006828_1448
19/08/01 05:43:34 INFO mapreduce.Job: The url to track the job: *******
19/08/01 05:43:34 INFO mapreduce.Job: Running job: job_1563260006828_1448
19/08/01 05:43:43 INFO mapreduce.Job: Job job_1563260006828_1448 running in uber mode : false
19/08/01 05:43:43 INFO mapreduce.Job:  map 0% reduce 0%
19/08/01 05:43:50 INFO mapreduce.Job:  map 100% reduce 0%
19/08/01 05:43:56 INFO mapreduce.Job:  map 100% reduce 100%
19/08/01 05:43:56 INFO mapreduce.Job: Job job_1563260006828_1448 completed successfully
19/08/01 05:43:56 INFO mapreduce.Job: Counters: 52
	File System Counters
		FILE: Number of bytes read=325
		FILE: Number of bytes written=475772
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=415
		HDFS: Number of bytes written=331
		HDFS: Number of read operations=9
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters
		Launched map tasks=2
		Launched reduce tasks=1
		Other local map tasks=1
		Rack-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=27663
		Total time spent by all reduces in occupied slots (ms)=14204
		Total time spent by all map tasks (ms)=9221
		Total time spent by all reduce tasks (ms)=3551
		Total vcore-milliseconds taken by all map tasks=9221
		Total vcore-milliseconds taken by all reduce tasks=3551
		Total megabyte-milliseconds taken by all map tasks=56653824
		Total megabyte-milliseconds taken by all reduce tasks=29089792
	Map-Reduce Framework
		Map input records=8
		Map output records=12
		Map output bytes=295
		Map output materialized bytes=331
		Input split bytes=259
		Combine input records=0
		Combine output records=0
		Reduce input groups=2
		Reduce shuffle bytes=331
		Reduce input records=12
		Reduce output records=8
		Spilled Records=24
		Shuffled Maps =2
		Failed Shuffles=0
		Merged Map outputs=2
		GC time elapsed (ms)=156
		CPU time spent (ms)=7700
		Physical memory (bytes) snapshot=2302410752
		Virtual memory (bytes) snapshot=23809572864
		Total committed heap usage (bytes)=5759303680
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	com.tarento.category.mr.SentenceCategorizer$CategorizerMapper$Counter
		CATEGORIZED_SENTENCES=4
		INPUT_SENTENCES=4
	File Input Format Counters
		Bytes Read=156
	File Output Format Counters
		Bytes Written=331


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Intermediate Similarity Score (for each category) File Generation.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[aravinth@local ~]$ hadoop fs -text /user/aravinth/out/part-r-00000
this is a mouse	animals	0.5858709332013085
this is a horse	animals	0.6705241461314837
this is a rabbit	animals	0.15151832300124213
this is a cat	animals	0.868752236323907
this is a mouse	cars	0.8805081383265995
this is a horse	cars	0.8428362335352474
this is a rabbit	cars	0.7950841162486014
this is a cat	cars	0.28443839880094235

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Kick to pick the best category (Highest Similarity Score) for a given sentence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[aravinth@local ~]$ hadoop jar /tmp/category-mr-1.0-jar-with-dependencies.jar com.tarento.category.mr.SentenceCategorizerPostProcess hdfs://hdfs-cluster/user/aravinth/out/part-r-00000 hdfs://hdfs-cluster/user/aravinth/final-cat

19/08/01 05:46:00 INFO client.AHSProxy: Connecting to Application History server at *******
19/08/01 05:46:00 INFO client.RequestHedgingRMFailoverProxyProvider: Looking for the active RM in [rm1, rm2]...
19/08/01 05:46:00 INFO client.RequestHedgingRMFailoverProxyProvider: Found active RM [rm1]
19/08/01 05:46:01 INFO input.FileInputFormat: Total input paths to process : 1
19/08/01 05:46:01 INFO mapreduce.JobSubmitter: number of splits:1
19/08/01 05:46:01 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1563260006828_1449
19/08/01 05:46:01 INFO impl.YarnClientImpl: Submitted application application_1563260006828_1449
19/08/01 05:46:01 INFO mapreduce.Job: The url to track the job: *******
19/08/01 05:46:01 INFO mapreduce.Job: Running job: job_1563260006828_1449
19/08/01 05:46:12 INFO mapreduce.Job: Job job_1563260006828_1449 running in uber mode : false
19/08/01 05:46:12 INFO mapreduce.Job:  map 0% reduce 0%
19/08/01 05:46:18 INFO mapreduce.Job:  map 100% reduce 0%
19/08/01 05:46:24 INFO mapreduce.Job:  map 100% reduce 100%
19/08/01 05:46:25 INFO mapreduce.Job: Job job_1563260006828_1449 completed successfully
19/08/01 05:46:25 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=272
		FILE: Number of bytes written=315386
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=449
		HDFS: Number of bytes written=86
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters
		Launched map tasks=1
		Launched reduce tasks=1
		Rack-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=13005
		Total time spent by all reduces in occupied slots (ms)=13668
		Total time spent by all map tasks (ms)=4335
		Total time spent by all reduce tasks (ms)=3417
		Total vcore-milliseconds taken by all map tasks=4335
		Total vcore-milliseconds taken by all reduce tasks=3417
		Total megabyte-milliseconds taken by all map tasks=26634240
		Total megabyte-milliseconds taken by all reduce tasks=27992064
	Map-Reduce Framework
		Map input records=8
		Map output records=8
		Map output bytes=250
		Map output materialized bytes=272
		Input split bytes=118
		Combine input records=0
		Combine output records=0
		Reduce input groups=4
		Reduce shuffle bytes=272
		Reduce input records=8
		Reduce output records=4
		Spilled Records=16
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=109
		CPU time spent (ms)=3070
		Physical memory (bytes) snapshot=2780733440
		Virtual memory (bytes) snapshot=16476172288
		Total committed heap usage (bytes)=3186098176
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters
		Bytes Read=331
	File Output Format Counters
		Bytes Written=86

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~ Final Categorization File Generation.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

[aravinth@local ~]$ hadoop fs -ls /user/aravinth/final-cat
Found 2 items
-rw-r--r--   3 aravinth hdfs          0 2019-08-01 05:46 /user/aravinth/final-cat/_SUCCESS
-rw-r--r--   3 aravinth hdfs         86 2019-08-01 05:46 /user/aravinth/final-cat/part-r-00000


[aravinth@local ~]$ hadoop fs -text /user/aravinth/final-cat/part-r-00000
this is a cat	animals
this is a horse	cars
this is a mouse	cars
this is a rabbit	cars
