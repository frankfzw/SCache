# SCache
A distributed memory cache system for shuffle in map-reduce

## How to Use

1. `sbt publishM2` Publish SCache jar to local maven repository.

2. `sbt assembly` Create fat jar of SCache

3. Add IP or hostname of slaves in `conf/slaves`

4. `sbin/copy-dir.sh` Distribute the code to cluster.

5. Build hadoop from [here](https://github.com/frankfzw/hadoop/tree/scache)

6. Edit `etc/hadoop/mapred-site.xml`, set `mapreduce.job.map.output.collector.class` to `org.apache.hadoop.mapred.MapTask$ScacheOutputBuffer` and `mapreduce.scache.home` to `your/scache/home`

6. Copy `config-1.2.1.jar`, `scache_2.11-0.1-SNAPSHOT.jar` and `scala-library.jar` to `hadoop-home/share/hadoop/yarn/lib`. You can find these jars in local maven/ivy repository and local scala home.

7. Distribute hadoop code in cluster.

8. `sbin/start-scache.sh` Start SCache

9. Start hadoop and submit jobs.
