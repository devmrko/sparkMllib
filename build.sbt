name := "SparkApp"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided" excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson")
  ),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided" excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson")
  ),
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.2" % "provided" excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson")
  ),
  "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided" excludeAll(
    ExclusionRule(organization = "javax.servlet")
  ),
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided" excludeAll(
    ExclusionRule(organization = "javax.servlet")
  ),
  "mysql" % "mysql-connector-java" % "5.1.38"
).map(_.exclude("commons-logging", "commons-logging")
	   .exclude("org.slf4j", "org.slf4j"))

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"