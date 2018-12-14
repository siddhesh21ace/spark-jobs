## Rep Join using Spark RDD

CS6240 - Fall 2018 Assignment 1 

### Author(s)

* **Siddhesh Salgaonkar**

### Installation

Check if these components are installed:

- JDK 1.8
- Scala 2.11.12
- Hadoop 2.9.1
- Spark 2.3.1 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

### Environment 

* Example ~/.bash_aliases:
```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export SCALA_HOME=/home/joe/tools/scala/scala-2.11.12
export SPARK_HOME=/home/joe/tools/spark/spark-2.3.1-bin-without-hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
```

### Execution
All of the build & execution commands are organized in the Makefile.

1. Open command prompt.
2. Make sure you are present in the root of the project directory.
3. Edit the Makefile to customize the environment at the top.  
   Sufficient for standalone: `hadoop.root, jar.name, local.input`  
   Other defaults acceptable for running standalone.
4. Standalone Hadoop:  

    ```
    make switch-standalone    // sets standalone Hadoop environment (execute once)
    make local
	```
	
5. [Pseudo-Distributed Hadoop](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	
	```
	make switch-pseudo			// sets pseudo-clustered Hadoop environment (execute once)
	make pseudo					// first execution
	make pseudoq				// later executions since namenode and datanode already running
	```
	 
6. AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	```
	make upload-input-aws		// only before first execution
	make aws					// check for successful execution with web interface (aws.amazon.com)
	download-output-aws			// after successful execution & termination
	```