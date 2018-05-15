name := "smart_plugs"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  DefaultMavenRepository, Resolver.sonatypeRepo("public")
)

libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "org.joda" % "joda-convert" % "2.0.1"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion)
libraryDependencies ++= Seq(
  "net.objectlab.kit" % "datecalc-common" % "1.4.0",
  "net.objectlab.kit" % "datecalc-jdk8" % "1.4.0"
)
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
