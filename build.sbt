name := "SCache"

organization := "org.sjtu"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "com.typesafe" % "config" % "1.2.1",
  "com.google.guava" % "guava" % "18.0",
  "com.google.code.findbugs" % "jsr305" % "3.0.0"
)
