# SCache

## System Overview

SCache is a distributed memory cache system that particularly focuses on shuffle optimization. By extracting and analyzing shuffle dependencies prior to the actual task execution, SCache can adopt heuristic pre-scheduling combining with shuffle size prediction to pre-fetch shuffle data and balance load on each node. Meanwhile, SCache takes full advantage of the system memory to accelerate the shuffle process.

<p align="center"><img src="https://github.com/wuchunghsuan/SCache/blob/master/fig/workflow.png" width="50%"/></p>
<p align="center">Figure 1:  Workflow Comparison between Legacy DAG Computing Frameworks and Frameworks with SCache</p>

SCache consists of three components: a distributed shuffle data management system, a DAG co-scheduler, and a worker daemon. As a plug-in system, SCache needs to rely on a DAG framework. As shown in Figure 2, SCache employs the legacy master-slaves architecture like GFS for the shuffle data management system. The master node of SCache coordinates the shuffle blocks globally with ap- plication context. The worker node reserves memory to store blocks. The coordination provides two guarantees: (a) data is stored in memory before tasks start and (b) data is scheduled on-off memory with all-or-nothing and context- aware constraints. The daemon bridges the communication between DAG framework and SCache. The co-scheduler is dedicated to pre-schedule reduce tasks with DAG information and enforce the scheduling results to original scheduler in framework.

For more detail, you can read our paper: [Efficient shuffle management with scache for dag computing frameworks](https://dl.acm.org/citation.cfm?id=3178510)

<p align="center"><img src="https://github.com/wuchunghsuan/SCache/blob/master/fig/architecture.png" width="50%"/></p>
<p align="center">Figure 2: SCache Architecture</p>

## Performance

We reveal the performance of SCache with comprehensive workloads and benchmarks.
1. Firstly, we run a job with single shuffle to analyze hardware utilization and see the impacts of different components from the scope of a task to a job. The performance evaluation in Figure 15 shows the con- sistent results with our observation of hardware utilization. For each stage, we pick the task that has median completion time.

    In the map task, the disk operations are replaced by the memory copies to decouple the shuffle write. It helps eliminate 40% of shuffle write time (Figure 4a), which leads to a 10% improvement of map stage completion time in Figure 3a.

    In the reduce task, most of the shuffle overhead is introduced by network transfer delay. By doing shuffle data pre-fetching based on the pre-scheduling results, the explicit network transfer is perfectly overlapped in the map stage. As a result, the combination of these optimizations decreases 100% overhead of the shuffle read in a reduce task (Figure 4b). In addition, the heuristic algorithm can achieve a balanced pre-scheduling result, thus providing 80% improvement in reduce stage completion time (Figure 3b).

    In overall, SCache can help Spark decrease by 89% overhead of the whole shuffle process.

<p align="center"><img src="https://github.com/wuchunghsuan/SCache/blob/master/fig/perf1.png" width="85%"/></p>
<p align="center">Figure 3 & 4: Stage Completion Time and Median Task Completion Time of Single Shuffle Test</p>

2. Secondly, we use a recognized shuffle intensive benchmark — Terasort to evaluate SCache with different data partition schemes.

    Terasort consists of two consecutive shuffles. The first shuffle reads the input data and uses a hash partition function for re-partitioning. As shown in Figure 5a, Spark with SCache runs 2 × faster during the reduce stage of the first shuffle. It further proves the effectiveness of SCache’s optimization.

<p align="center"><img src="https://github.com/wuchunghsuan/SCache/blob/master/fig/terasort.png" width="30%"/></p>
<p align="center">Figure 5: Terasort Evaluation</p>

3. At last, in order to prove the performance gain of SCache with a real production workload, we evaluate Spark [TPC-DS](https://github.com/databricks/spark-sql-perf) and present the overall performance improvement.

    TPC-DS benchmark is designed for modeling multiple users sub- mitting varied queries. TPC-DS contains 99 queries and is considered as the standardized industry benchmark for testing big data systems. As shown in Figure 6, the horizontal axis is query name and the vertical axis is query completion time. The overall reduction portion of query time that SCache achieved is 40% on average. Since this evaluation presents the overall job completion time of queries, we believe that our shuffle optimization is promising.

<p align="center"><img src="https://github.com/wuchunghsuan/SCache/blob/master/fig/tpc-ds.png" width="85%"/></p>
<p align="center">Figure 6: TPC-DS Benchmark Evaluation</p>

## How to Use

1. `sbt publishM2` Publish SCache jar to local maven repository.

2. `sbt assembly` Create fat jar of SCache

3. Add IP or hostname of slaves in `conf/slaves`

4. `sbin/copy-dir.sh` Distribute the code to cluster.

5. Build Hadoop from [here](https://github.com/frankfzw/hadoop/tree/scache)

6. Edit `etc/hadoop/mapred-site.xml`, set `mapreduce.job.map.output.collector.class` to `org.apache.hadoop.mapred.MapTask$ScacheOutputBuffer` , set `mapreduce.job.reduce.shuffle.consumer.plugin.class` to `org.apache.hadoop.mapreduce.task.reduce.ScacheShuffle` and set `mapreduce.scache.home` to `your/scache/home`

6. Copy `config-1.2.1.jar`, `scache_2.11-0.1-SNAPSHOT.jar` and `scala-library.jar` to `hadoop-home/share/hadoop/yarn/lib`. You can find these jars in local maven/ivy repository and local scala home.

7. Distribute hadoop code in cluster.

8. `sbin/start-scache.sh` Start SCache

9. Start Hadoop and submit jobs.
