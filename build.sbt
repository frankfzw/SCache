name := "SCache"

organization := "org.scache"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "com.typesafe" % "config" % "1.2.1",
  "com.google.guava" % "guava" % "18.0",
  "com.google.code.findbugs" % "jsr305" % "3.0.0",
  "io.netty" % "netty-all" % "4.0.29.Final",
  "io.netty" % "netty" % "3.8.0.Final",
  "com.esotericsoftware.kryo" % "kryo" % "2.24.0",
  "org.apache.avro" % "avro" % "1.7.7",
  "commons-io" % "commons-io" % "2.4",
  "com.ning" % "compress-lzf" % "1.0.3",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "org.eclipse.jetty" % "jetty-util" % "9.2.16.v20160414",
  "com.twitter" % "chill_2.10" % "0.8.0",
  "com.twitter" % "chill-java" % "0.8.0",
  "org.roaringbitmap" % "RoaringBitmap" % "0.5.11",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0"
)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
