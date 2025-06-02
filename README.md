# Loan Processing System

A microservices-based loan processing system that leverages event-driven architecture with Kafka for asynchronous communication and AI models for decision making.

<img width="1703" alt="Screenshot 2025-06-02 at 1 18 37 AM" src="https://github.com/user-attachments/assets/753a3232-532f-403b-8065-6b2e96d3c04c" />


## Overview

This system processes loan applications through a series of microservices that evaluate income prediction, non-performing loan (NPL) risk, and ultimately make loan decisions. The architecture uses both synchronous API calls and asynchronous event-driven patterns to ensure efficient and scalable loan processing.

## Architecture

### Microservices

| Service | Acronym | Description |
|---------|---------|-------------|
| **Loan Application Service** | LAS | Handles loan application submissions from various channels (Mobile, Web, Branch UI) |
| **Loan Decision Service** | LDES | Core service that orchestrates the loan decision process |
| **Income Prediction Service** | IPS | AI-powered service for predicting applicant income |
| **NPL Prediction Service** | NPL | AI model for predicting Non-Performing Loan risk |
| **Notification Service** | NS | Sends notifications to applicants about loan decisions |
| **Loan Disbursement Service** | LDIS | Handles the actual loan disbursement process |

### External Systems

- **Data Warehouse (DWH)** - Hive-based system storing customer demographics and pre-calculated income predictions
- **Mobile Banking App** - Customer interface for loan applications
- **Web Banking App** - Web-based customer interface
- **Branch UI** - Internal interface for branch staff

## Event-Driven Architecture

### Kafka Topics and Events

| Event | Producer | Consumer | Kafka Topic | Description |
|-------|----------|----------|-------------|-------------|
| event1 | LDES | IPS | income_prediction_request | Income prediction request for prospect customers (real-time processing) |
| event2 | IPS | NPL | income_prediction_result | Result of income prediction evaluation |
| event3 | LDES | NPL | npl_prediction_request | Request for Non-Performing Loan risk assessment |
| event4 | NPL | LDES | npl_prediction_result | NPL risk assessment result |
| event5 | LDES | NS | loan_decision_result_notification | Final loan decision for notification |
| event6 | LDES | LDIS | loan_decision_result_disbursement | Loan decision for disbursement processing |

## Process Flow

### 1. Loan Application Submission
- Users submit applications via Mobile/Web Banking Apps or Branch UI
- LAS processes the request and makes a synchronous API call to LDES
- LAS retrieves demographic data from DWH (Hive) for both customers and prospects
- Pre-calculated income predictions are fetched for existing customers only

### 2. Income Prediction
- **For Prospect Customers**: Real-time processing via IPS
  - LDES publishes `income_prediction_request` event
  - IPS consumes, evaluates, and publishes `income_prediction_result`
- **For Existing Customers**: Uses pre-calculated predictions from DWH (daily batch process)

### 3. NPL Risk Assessment
- LDES publishes `npl_prediction_request` event
- NPL service consumes:
  - NPL prediction requests from `npl_prediction_request` topic
  - Income predictions from `income_prediction_result` topic (for prospects)
  - For existing customers, income data is passed directly by LDES (from DWH API call)
- NPL evaluates and publishes risk assessment to `npl_prediction_result`

### 4. Loan Decision
- LDES consumes `npl_prediction_result` events
- Makes final loan decision based on all inputs
- Publishes decision to both notification and disbursement topics

### 5. Notification
- NS consumes `loan_decision_result_notification` events
- Sends notifications via email/SMS/push notifications
- Notifies applicants of approval, rejection, or need for further review

### 6. Disbursement
- LDIS consumes `loan_decision_result_disbursement` events
- For approved loans: Processes payment via API call
- For rejected loans: No action taken

## Database Operations

| Event | Service | DB Operations |
|-------|---------|---------------|
| event1 | IPS | Read: income_prediction |
| event2 | IPS | Write: income_prediction |
| event2 | NPL | Read: npl_prediction |
| event3 | NPL | Read: npl_prediction |
| event4 | NPL | Write: npl_prediction |
| event4 | LDES | Read: loan_result |
| event5/6 | LDES | Write: loan_result |

## Key Features

- **Asynchronous Processing**: Event-driven architecture ensures scalability and resilience
- **Real-time vs Batch Processing**: Optimizes performance based on customer type
- **Multi-Channel Support**: Accepts applications from mobile, web, and branch interfaces
- **Comprehensive Audit Trail**: All events and decisions are logged for compliance
- **Stream Processing**: Apache Flink-based real-time data processing
- **Multi-Database Support**: PostgreSQL, Cassandra, and time-series databases
- **Advanced Analytics**: Integration with Grafana, Kibana, and Tableau

## Technology Stack

### Core Technologies
- **Language**: Scala 2.12.10
- **Stream Processing**: Apache Flink 1.13.2
- **Message Broker**: Apache Kafka
- **Serialization**: Apache Avro 1.10.2
- **JSON Processing**: json4s-jackson 4.0.6

### Databases
- **PostgreSQL** 42.2.2 - Primary relational database
- **Apache Cassandra** - Distributed NoSQL for high-volume data
- **InfluxDB** - Time-series data for metrics and monitoring

### Analytics & Monitoring
- **Grafana** - Real-time dashboards via InfluxDB integration
- **Elasticsearch** 7.10.2 - Log aggregation and search
- **Kibana** - Log visualization and analysis
- **Tableau** - Business intelligence via CSV exports

### Observability
- **Logging**: Logback 1.2.10
- **Metrics**: Flink Metrics with Dropwizard
- **HTTP Monitoring**: OkHttp 4.9.3 for REST API integrations

## Prerequisites

- Apache Kafka cluster
- Java 11+ runtime environment
- Scala 2.12.10
- Apache Flink 1.13.2 cluster
- PostgreSQL 42.2.2+
- Cassandra cluster (optional)
- InfluxDB for metrics (optional)
- Elasticsearch cluster for logging (optional)
- Access to Data Warehouse (Hive)
- AI/ML model infrastructure for IPS and NPL services

## Installation

### Prerequisites Setup

1. **Install Java 8 or 11**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install openjdk-11-jdk
   
   # macOS
   brew install openjdk@11
   ```

2. **Install SBT**
   ```bash
   # Ubuntu/Debian
   echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
   echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
   curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
   sudo apt-get update
   sudo apt-get install sbt
   
   # macOS
   brew install sbt
   ```

3. **Install Apache Flink 1.13.2**
   ```bash
   wget https://archive.apache.org/dist/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.12.tgz
   tar -xzf flink-1.13.2-bin-scala_2.12.tgz
   cd flink-1.13.2
   ./bin/start-cluster.sh
   ```

### Project Setup

```bash
# Clone the repository
git clone https://github.com/your-org/scala-realtime-loan-allocation.git
cd scala-realtime-loan-allocation

# Download dependencies and compile
sbt update
sbt compile

# Run tests
sbt test

# Create fat JAR with all dependencies
sbt assembly

# Run specific microservice (example)
sbt "run com.loanprocessing.LoanDecisionService"
```

### Docker Deployment (Recommended)

```bash
# Build Docker images for all services
docker build -t loan-processing/las -f docker/Dockerfile.las .
docker build -t loan-processing/ldes -f docker/Dockerfile.ldes .
docker build -t loan-processing/ips -f docker/Dockerfile.ips .
docker build -t loan-processing/npl -f docker/Dockerfile.npl .
docker build -t loan-processing/ns -f docker/Dockerfile.ns .
docker build -t loan-processing/ldis -f docker/Dockerfile.ldis .

# Start all services with docker-compose
docker-compose up -d
```

## Build Configuration

The project uses SBT with the following `build.sbt` configuration:

```scala
name := "scala-realtime-loan-allocation"
version := "0.1"
scalaVersion := "2.12.10"
```

### Dependency Groups

1. **Flink Core Dependencies**
   - flink-clients, flink-scala, flink-streaming-scala (1.13.2)
   - flink-table-api-scala-bridge, flink-table-planner-blink
   - Apache Avro (1.10.2) for serialization
   - json4s-jackson (4.0.6) for JSON processing

2. **Flink Connectors**
   - flink-connector-kafka (with Avro exclusion)
   - flink-connector-cassandra
   - flink-connector-jdbc
   - PostgreSQL driver (42.2.2)

3. **Analytics & Monitoring**
   - InfluxDB Java Client (6.7.0) - Grafana integration
   - Elasticsearch REST Client (7.10.2) - Kibana integration
   - Flink Metrics Dropwizard
   - OpenCSV (5.6) - Tableau exports
   - Commons Math3 (3.6.1) - Statistical analysis
   - OkHttp3 (4.9.3) - REST API calls

4. **Logging**
   - Logback Core & Classic (1.2.10)

### Troubleshooting Dependencies

If you encounter dependency conflicts:
```bash
# Check dependency tree
sbt dependencyTree

# Clean and rebuild
sbt clean compile

# Update specific dependencies
sbt update
```

## Configuration

Each microservice requires configuration for:
- Kafka broker connections
- Database connections (PostgreSQL, Cassandra)
- Flink cluster settings
- API endpoints (DWH, payment systems)
- Metrics backends (InfluxDB, Elasticsearch)
- Service-specific parameters

Example configuration structure:
```yaml
kafka:
  bootstrap-servers: localhost:9092
  consumer-group: ${SERVICE_NAME}-group
  serialization: avro

flink:
  parallelism: 4
  checkpoint-interval: 60000
  state-backend: rocksdb

database:
  postgres:
    url: jdbc:postgresql://localhost:5432/${SERVICE_NAME}_db
    username: ${DB_USER}
    password: ${DB_PASSWORD}
  
  cassandra:
    hosts: ["localhost"]
    port: 9042
    keyspace: loan_processing

metrics:
  influxdb:
    url: http://localhost:8086
    database: loan_metrics
  
  elasticsearch:
    hosts: ["localhost:9200"]
    index: loan-logs

api:
  dwh-endpoint: http://dwh-api:8080
  timeout: 30s
```

## Development

### Project Structure
```
scala-realtime-loan-allocation/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/loanprocessing/
│   │   │       ├── las/          # Loan Application Service
│   │   │       ├── ldes/         # Loan Decision Service
│   │   │       ├── ips/          # Income Prediction Service
│   │   │       ├── npl/          # NPL Prediction Service
│   │   │       ├── ns/           # Notification Service
│   │   │       └── ldis/         # Loan Disbursement Service
│   │   └── resources/
│   │       ├── application.conf
│   │       └── logback.xml
│   └── test/
│       └── scala/
└── docker/
    ├── docker-compose.yml
    └── Dockerfile.*
```

### Running Individual Services

```bash
# Start Flink cluster first
$FLINK_HOME/bin/start-cluster.sh

# Run specific service with SBT
sbt "runMain com.loanprocessing.ldes.LoanDecisionServiceApp"

# Or submit to Flink cluster
$FLINK_HOME/bin/flink run -c com.loanprocessing.ldes.LoanDecisionServiceApp \
  target/scala-2.12/scala-realtime-loan-allocation-assembly-0.1.jar

# Monitor Flink jobs
open http://localhost:8081
```



3. **Monitoring Setup**
   - Configure Flink metrics reporter for InfluxDB
   - Set up Grafana dashboards
   - Configure log aggregation to Elasticsearch
   - Set up alerts for critical metrics

### Real-time Dashboards
- **Grafana**: Connect to InfluxDB for real-time metrics visualization
  - Loan processing rates
  - Decision latency metrics

