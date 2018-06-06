name := "smart_plugs"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  DefaultMavenRepository, Resolver.sonatypeRepo("public")
)

val depManagement = "provided"


libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.9.9",
  "org.joda" % "joda-convert" % "2.0.1",
  "org.scalactic" %% "scalactic" % "3.0.1"  ,
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "de.jollyday" % "jollyday" % "0.5.5",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" ,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "org.alluxio" % "alluxio-core-client-hdfs" % "1.7.1"
)
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("com", "fasterxml", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "module-info.class" => MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
