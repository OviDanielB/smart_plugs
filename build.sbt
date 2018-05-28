name := "smart_plugs"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  DefaultMavenRepository, Resolver.sonatypeRepo("public")
)

resolvers += "Apache HBase" at
  "https://repository.apache.org/content/repositories/releases"
resolvers += "Thrift" at "https://people.apache.org/~rawson/repo/"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.9.9",
  "org.joda" % "joda-convert" % "2.0.1",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "de.jollyday" % "jollyday" % "0.5.5",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "org.apache.hbase" % "hbase-server" % "1.3.1",
  "org.apache.hbase" % "hbase-client" % "1.3.1",
  "org.apache.hbase" % "hbase-common" % "1.3.1"
)