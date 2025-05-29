name := "scala-realtime-loan-allocation"

version := "0.1"

scalaVersion := "2.12.10"

val flinkVersion = "1.13.2"
val postgresVersion = "42.2.2"
val logbackVersion = "1.2.10"
val elasticsearchVersion = "7.10.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
  "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion,
  "org.apache.avro" % "avro" % "1.10.2",
  "org.json4s" %% "json4s-jackson" % "4.0.6"
)

val flinkConnectors = Seq(
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion excludeAll(ExclusionRule(organization = "org.apache.avro")),
  "org.apache.flink" %% "flink-connector-cassandra" % flinkVersion,
  "org.apache.flink" %% "flink-connector-jdbc" % flinkVersion,
  "org.postgresql" % "postgresql" % postgresVersion
)

// Analytics and Visualization Dependencies
val analyticsDependencies = Seq(
  // InfluxDB connector for Grafana dashboards
  "com.influxdb" % "influxdb-client-java" % "6.7.0",

  // Elasticsearch connector for Kibana dashboards (optional)
  "org.apache.flink" %% "flink-connector-elasticsearch7" % flinkVersion,
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticsearchVersion,

  // Metrics (only include ones that exist)
  "org.apache.flink" % "flink-metrics-dropwizard" % flinkVersion,

  // CSV export for Tableau integration
  "com.opencsv" % "opencsv" % "5.6",

  // Time series analysis
  "org.apache.commons" % "commons-math3" % "3.6.1",

  // HTTP client for REST API integrations
  "com.squareup.okhttp3" % "okhttp" % "4.9.3"
)

val logging = Seq(
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion
)



libraryDependencies ++= flinkDependencies ++ flinkConnectors ++ analyticsDependencies ++ logging