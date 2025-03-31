name := "scala-realtime-loan-allocation"

version := "0.1"

scalaVersion := "2.12.10"

val flinkVersion = "1.13.2"
val postgresVersion = "42.2.2"
val logbackVersion = "1.2.10"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.avro" % "avro" % "1.10.2",
  "org.json4s" %% "json4s-jackson" % "4.0.6"
)

val flinkConnectors = Seq(
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion excludeAll(ExclusionRule(organization = "org.apache.avro")),
  "org.apache.flink" %% "flink-connector-cassandra" % flinkVersion,
  "org.apache.flink" %% "flink-connector-jdbc" % flinkVersion,
  "org.postgresql" % "postgresql" % postgresVersion
)

val logging = Seq(
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion
)

libraryDependencies ++= flinkDependencies ++ flinkConnectors ++ logging