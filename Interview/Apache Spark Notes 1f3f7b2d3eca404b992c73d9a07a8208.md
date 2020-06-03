# Apache Spark Notes

Created By: Akshay Bahadur
Last Edited: Jun 03, 2020 1:19 AM

## Introduction

Apache Spark is an open-source cluster computing framework. Its primary purpose is to handle the real-time generated data.

Spark was built on the top of the Hadoop MapReduce. It was optimized to run in memory whereas alternative approaches like Hadoop's MapReduce writes data to and from computer hard drives. So, Spark process the data much quicker than other alternatives. Improvements over Map Reduce

- There is no tight coupling in Spark i.e., there is no mandatory rule that reduce must come after map.
- Spark tries to keep the data “in-memory” as much as possible.

## Spark Architecture

The Spark follows the master-slave architecture. Its cluster consists of a single master and multiple slaves.

The Spark architecture depends upon two abstractions:

- Resilient Distributed Dataset (RDD)
- Directed Acyclic Graph (DAG)

## Resilient Distributed Datasets (RDD)

RDD (Resilient Distributed Dataset) is the main logical data unit in Spark. An RDD has distributed a collection of objects. Distributed means, each RDD is divided into multiple partitions. Each of these
partitions can reside in memory or stored on the disk of different machines in a cluster. RDDs are immutable (Read-Only) data structure. You can’t change original RDD, but you can always transform it into different RDD with all changes you want.

## Directed Acyclic Graph (DAG)

Directed Acyclic Graph is a finite direct graph that performs a sequence of computations on data. Each node is an RDD partition, and the edge is a transformation on top of data. Here, the graph refers to the navigation whereas directed and acyclic refers to how it is done.

Let's understand the Spark architecture.

![https://static.javatpoint.com/tutorial/spark/images/spark-architecture.png](https://static.javatpoint.com/tutorial/spark/images/spark-architecture.png)

## Driver Program

The Driver Program is a process that runs the main() function of the application and creates the **SparkContext** object. The purpose of **SparkContext** is to coordinate the spark applications, running as independent sets of processes on a cluster.

To run on a cluster, the **SparkContext** connects to a different type of cluster managers and then perform the following tasks: -

- It acquires executors on nodes in the cluster.
- Then, it sends your application code to the executors. Here, the application code can be defined by JAR or Python files passed to the SparkContext.
- At last, the SparkContext sends tasks to the executors to run.

## Cluster Manager

- The role of the cluster manager is to allocate resources across applications. The Spark is capable enough of running on a large number of clusters.
- It consists of various types of cluster managers such as Hadoop YARN, Apache Mesos and Standalone Scheduler.
- Here, the Standalone Scheduler is a standalone spark cluster manager that facilitates to install Spark on an empty set of machines.

### Worker Node

- The worker node is a slave node
- Its role is to run the application code in the cluster.

### Executor

- An executor is a process launched for an application on a worker node.
- It runs tasks and keeps data in memory or disk storage across them.
- It read and write data to the external sources.
- Every application contains its executor.

### Task

- A unit of work that will be sent to one executor.

# Spark Components

The Spark project consists of different types of tightly integrated components. At its core, Spark is a computational engine that can schedule, distribute and monitor multiple applications. Let's understand each Spark component in detail.

![https://static.javatpoint.com/tutorial/spark/images/spark-components.png](https://static.javatpoint.com/tutorial/spark/images/spark-components.png)

## Spark Core

- The Spark Core is the heart of Spark and performs the core functionality.
- It holds the components for task scheduling, fault recovery, interacting with storage systems and memory management.

## Spark SQL

- The Spark SQL is built on the top of Spark Core. It provides support for structured data.
- It allows to query the data via SQL (Structured Query Language) as well as the Apache Hive variant of SQL?called the HQL (Hive Query Language).
- It supports JDBC and ODBC connections that establish a relation between Java objects and existing databases, data warehouses and business intelligence tools.
- It also supports various sources of data like Hive tables, Parquet, and JSON.

## Spark Streaming

- Spark Streaming is a Spark component that supports scalable and fault-tolerant processing of streaming data.
- It uses Spark Core's fast scheduling capability to perform streaming analytics.
- It accepts data in mini-batches and performs RDD transformations on that data.
- Its design ensures that the applications written for streaming data can be reused to analyze batches of historical data with little modification.
- The log files generated by web servers can be considered as a real-time example of a data stream.

## MLlib

- The MLlib is a Machine Learning library that contains various machine learning algorithms.
- These include correlations and hypothesis testing, classification and regression, clustering, and principal component analysis.
- It is nine times faster than the disk-based implementation used by Apache Mahout.

## GraphX

- The GraphX is a library that is used to manipulate graphs and perform graph-parallel computations.
- It facilitates to create a directed graph with arbitrary properties attached to each vertex and dge.
- To manipulate graph, it supports various fundamental operators like subgraph, join Vertices, and aggregate Messages.

# RDD Operations

The RDD provides the two types of operations:

- Transformation - In Spark, the role of transformation is to create a new dataset from an existing one. The transformations are considered lazy as they only computed when an action requires a result to be returned to the driver program.
- Action - In Spark, the role of action is to return a value to the driver program after running a computation on the dataset.

# RDD Persistence

Spark provides a convenient way to work on the dataset by persisting it in memory across operations. While persisting an RDD, each node stores any partitions of it that it computes in memory. Now, we can also reuse them in other tasks on that dataset.

We can use either persist() or cache() method to mark an RDD to be persisted. Spark?s cache is fault-tolerant. In any case, if the partition of an RDD is lost, it will automatically be recomputed using the transformations that originally created it.

There is an availability of different storage levels that are used to store persisted RDDs. Use these levels by passing a **StorageLevel** object (Scala, Java, Python) to persist(). However, the cache() method is used for the default storage level, which is StorageLevel.MEMORY_ONLY.

## Interview Questions

### **What is the role of coalesce () and repartition () in Map Reduce?**

Both coalesce and repartition are used to modify the number of partitions in an RDD but Coalesce avoids full shuffle. If you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead, each of the 100 new partitions will claim 10 of the current partitions and this does not require a shuffle.

Repartition performs a coalesce with shuffle. Repartition will result in the specified number of partitions with the data distributed using a hash practitioner.

### **What are Accumulators?**

**A.** Accumulators are the write-only variables which are initialized once and sent to the workers. These workers will update based on the logic written and sent back to the driver which will aggregate or process based on the logic.

Only driver can access the accumulator’s value. For tasks, Accumulators are write-only. For example, it is used to count the number errors seen in RDD across workers.

### **What are the optimizations that the developer can make while working with spark?**

- Spark is memory intensive, whatever you do it does in memory.
- Firstly, you can adjust how long spark will wait before it times out on each of the phases of data locality (data local –> process local –> node local –> rack local –> Any).
- Filter out data as early as possible. For caching, choose wisely from various storage levels.
- Tune the number of partitions in spark.

### **What is Executor Memory in a Spark application?**

Every spark application has the same fixed heap size and a fixed number of cores for a spark executor. The heap size is what referred to as the Spark executor memory which is controlled with the spark.executor.memory property of the ***–executor-memory*** flag. Every spark application will have one executor on each worker node. The executor memory is basically a measure of how much memory of the worker node will the application utilizes.

### **How is Streaming implemented in Spark? Explain with examples.**

*Spark Streaming* is used for processing real-time streaming data. Thus it is a useful addition to the core Spark API. It enables high-throughput and fault-tolerant stream processing of live data streams. The fundamental stream unit is DStream which is basically a series of RDDs (Resilient
Distributed Datasets) to process the real-time data. The data from different sources like Flume, HDFS is streamed and finally processed to file systems, live dashboards, and databases. It is similar to batch processing as the input data is divided into streams like batches.

![Apache%20Spark%20Notes%201f3f7b2d3eca404b992c73d9a07a8208/Untitled.png](Apache%20Spark%20Notes%201f3f7b2d3eca404b992c73d9a07a8208/Untitled.png)

Spark and Hadoop

### **How can you minimize data transfers when working with Spark?**

Minimizing data transfers and avoiding shuffling helps write spark programs that run in a fast and reliable manner. The various ways in which data transfers can be minimized when working with Apache Spark are:

1. Using Broadcast Variable- Broadcast variable enhances the efficiency of joins between small and large RDDs.
2. Using Accumulators – Accumulators help update the values of variables in parallel while executing.

The most common way is to avoid operations ByKey, repartition or any other operations which trigger shuffles.

### **What is the significance of Sliding Window operation?**

Sliding Window controls the transmission of data packets between various computer networks. Spark Streaming library provides windowed computations where the transformations on RDDs are applied over a sliding window of data. Whenever the window slides, the RDDs that fall within the particular window are combined and operated upon to produce new RDDs of the windowed DStream.

![https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2017/04/DStream-Sliding-Window-Spark-Interview-Questions-Edureka.png](https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2017/04/DStream-Sliding-Window-Spark-Interview-Questions-Edureka.png)

### **45. What is a DStream in Apache Spark?**

**Discretized Stream** (DStream) is the basic abstraction provided by Spark Streaming. It is a continuous stream of data. It is received from a data source or from a processed data stream generated by transforming the input stream. Internally, a DStream is represented by a continuous series of RDDs and each RDD contains data from a certain interval. Any operation applied on a DStream translates to operations on the underlying RDDs.

DStreams can be created from various sources like Apache Kafka, HDFS, and Apache Flume. DStreams have two operations:

1. Transformations that produce a new DStream.
2. Output operations that write data to an external system.

There are many DStream transformations possible in Spark Streaming. Let us look at **filter(func)**. filter(func) returns a new DStream by selecting only the records of the source DStream on which *func* returns true.

![https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2017/04/DStream-Filter-Spark-Interview-Questions-Edureka.png](https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2017/04/DStream-Filter-Spark-Interview-Questions-Edureka.png)

### **Does Apache Spark provide checkpoints?**

*Checkpoints* are similar to checkpoints in gaming. They make it run 24/7 and make it resilient to failures unrelated to the application logic.

![https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2017/04/Checkpoints-Spark-Interview-Questions-Edureka.png](https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2017/04/Checkpoints-Spark-Interview-Questions-Edureka.png)

**Figure:**

*Spark Interview Questions – Checkpoints*

Lineage graphs are always useful to recover RDDs from a failure but this is generally time-consuming if the RDDs have long lineage chains. Spark has an API for checkpointing i.e. a REPLICATE flag to persist. However, the decision on which data to checkpoint – is decided by the user.

### **What do you understand by SchemaRDD in Apache Spark RDD?**

*SchemaRDD* is an RDD that consists of row objects (wrappers around the basic string or integer arrays) with schema information about the type of data in each column. SchemaRDD was designed as an attempt to make life easier for developers in their daily routines of code debugging and unit testing on SparkSQL core module. The idea can boil down to describing the data structures inside
RDD using a formal description similar to the relational database schema. On top of all basic functions provided by common RDD APIs, SchemaRDD also provides some straightforward relational query interface functions that are realized through SparkSQL.

Now, it is officially renamed to *DataFrame API* on Spark’s latest trunk.

### **What is RDD Lineage?**

Spark does not
support data replication in memory and thus, if any data is lost, it is rebuild using RDD lineage. RDD lineage is a process that reconstructs lost data partitions. The best thing about this is that RDDs always remember how to build from other datasets.

### What is Map and flatMap in Spark?

The map is a specific line or row to process that data. In FlatMap each input item can be mapped to multiple output items (so the function should return a Seq rather than a single item).