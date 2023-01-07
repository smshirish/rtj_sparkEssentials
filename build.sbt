name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.2"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.14.1",
"com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
///  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.2",
  "com.lihaoyi" %% "upickle" % "0.7.1"
)