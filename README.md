# SCache
A distributed memory cache system that particularly focuses on shuffle optimization. By extracting and analyzing shuffle dependencies prior to the actual task execution, SCache can adopt heuristic pre-scheduling combining with shuffle size prediction to pre-fetch shuffle data and balance load on each node. Meanwhile, SCache takes full advantage of the system memory to accelerate the shuffle process.

SCache consists of three components: a distributed shuffle data management system, a DAG co-scheduler, and a worker daemon. As a plug-in system, SCache needs to rely on a DAG framework. As shown in belowed figure, SCache employs the legacy master-slaves architecture like GFS for the shuffle data management system. The master node of SCache coordinates the shuffle blocks globally with ap- plication context. The worker node reserves memory to store blocks. The coordination provides two guarantees: (a) data is stored in memory before tasks start and (b) data is scheduled on-off memory with all-or-nothing and context- aware constraints. The daemon bridges the communication between DAG framework and SCache. The co-scheduler is dedicated to pre-schedule reduce tasks with DAG information and enforce the scheduling results to original scheduler in framework.

![Architecture](https://github.com/wuchunghsuan/SCache/blob/master/fig/architecture.png)

## How to Use

1. `sbt publishM2` Publish SCache jar to local maven repository.

2. `sbt assembly` Create fat jar of SCache

3. Add IP or hostname of slaves in `conf/slaves`

4. `sbin/copy-dir.sh` Distribute the code to cluster.

5. Build hadoop from [here](https://github.com/frankfzw/hadoop/tree/scache)

6. Edit `etc/hadoop/mapred-site.xml`, set `mapreduce.job.map.output.collector.class` to `org.apache.hadoop.mapred.MapTask$ScacheOutputBuffer` , set `mapreduce.job.reduce.shuffle.consumer.plugin.class` to `org.apache.hadoop.mapreduce.task.reduce.ScacheShuffle` and set `mapreduce.scache.home` to `your/scache/home`

6. Copy `config-1.2.1.jar`, `scache_2.11-0.1-SNAPSHOT.jar` and `scala-library.jar` to `hadoop-home/share/hadoop/yarn/lib`. You can find these jars in local maven/ivy repository and local scala home.

7. Distribute hadoop code in cluster.

8. `sbin/start-scache.sh` Start SCache

9. Start hadoop and submit jobs.
