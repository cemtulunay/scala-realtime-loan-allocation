# Loan Processing System

A microservices-based loan processing system that leverages event-driven architecture with Kafka for asynchronous communication and constrained random data generation and randomly enriching data simulating "AI models" for decision making.


## Overview

This system processes loan applications through a series of microservices that evaluate income prediction (realtime for prospect customers, batch for customers), non-performing loan (NPL) risk, and ultimately make loan decisions. The architecture uses both synchronous API calls (API calls are imaginary) and asynchronous event-driven patterns to ensure efficient and scalable loan processing.

* MicorServices are imaginary.
* Data is generated radomly (constrained random data generation)
* Data enriched in microservices randomly (simulating AI as decision maker)
* Data produced and consumed by Flink and flow between the Microservices by Kafka Topics
* Finally after Data turn back to LDES ("Loan Decision Service") randomly (constrained random data enrichment) Loan Decision made with the inputs from income prediction then npl service
* "Loan Disbursement Service" actually receives the final data from LDES and use it for time series analysis with realt-time aggregations on a Grafana dashboard powered by influxDB
* "Loan Notification Service" works with final data similar as disbursement service. Provides realtime notifications and alerts

### External Systems (This part is imaginary and generated data used in place of real systems and values for illustrative purposes)

- **Data Warehouse (DWH)** - Hive-based system storing customer demographics and pre-calculated income predictions
- **Mobile Banking App** - Customer interface for loan applications
- **Web Banking App** - Web-based customer interface
- **Branch UI** - Internal interface for branch staff

### Microservices

| Service | Acronym | Description |
|---------|---------|-------------|
| **Loan Application Service** | LAS | Handles loan application submissions from various channels (Mobile, Web, Branch UI) |
| **Loan Decision Service** | LDES | Core service that orchestrates the loan decision process |
| **Income Prediction Service** | IPS | AI-powered service for predicting applicant income |
| **NPL Prediction Service** | NPL | AI model for predicting Non-Performing Loan risk |
| **Notification Service** | NS | Sends notifications to applicants about loan decisions |
| **Loan Disbursement Service** | LDIS | Handles the actual loan disbursement process |

## Event-Driven Architecture

<img width="1703" alt="Screenshot 2025-06-02 at 1 18 37 AM" src="https://github.com/user-attachments/assets/753a3232-532f-403b-8065-6b2e96d3c04c" />

### Kafka Topics and Events


| event  | source | target | kafka topic                          | Kafka Write        | Kafka Read         | DB               | DB write         | DB read                       | DB update                     |
|--------|--------|--------|--------------------------------------|--------------------|--------------------|------------------|------------------|-------------------------------|-------------------------------|
| event1 | LDES   | IPS    | income_prediction_request            | event1 - producer  | event1 - consumer  | income_prediction| event1-consumer  | event2-producer               | event2-producer               |
| event2 | IPS    | NPL    | income_prediction_result             | event2 - producer  | event2 - consumer  | npl_prediction   | event2-consumer  | event4-producer               | event4-producer               |
| event3 | LDES   | NPL    | npl_prediction_request               | event3 - producer  | event3 - consumer  | npl_prediction   | event2-consumer  | event4-producer               | event4-producer               |
| event4 | NPL    | LDES   | npl_prediction_result                | event4 - producer  | event4 - consumer  | loan_result      | event4-consumer  | event5-producer<br>event6-producer | event5-producer<br>event6-producer |
| event5 | LDES   | NS     | loan_decision_result_notification    | event5 - producer  | event5 - consumer  | loan_result      | event4-consumer  | event5-producer<br>event6-producer | event5-producer<br>event6-producer |
| event6 | LDES   | LDIS   | loan_decision_result_disbursement    | event6 - producer  | event6 - consumer  | loan_result      | event4-consumer  | event5-producer<br>event6-producer | event5-producer<br>event6-producer |


## Data Flow

### 1. Application Entry
- User submits loan application (Mobile/Web/Branch)
- **LAS** receives request
- LAS calls **DWH API** for demographics
- LAS sends sync request to **LDES**

---

### 2. Customer Type Check
- **LDES** checks if applicant is an existing customer
  - **Customer**: Uses pre-calculated income prediction from **DWH**
  - **Prospect**: Triggers real-time income prediction

---

### 3. Income Prediction (Prospects Only)
- `LDES → Flink (Producer) → Kafka → income_prediction_request`
- **IPS** consumes event
- IPS runs **AI model** (imaginary, just enriches data randomly)
- `IPS → Flink (Consumer) → Kafka → income_prediction_result`

---

### 4. NPL Risk Assessment
- `LDES → Flink (Producer) → Kafka → npl_prediction_request`
- **NPL** consumes request
- NPL retrieves income data:
  - **Prospect**: From `income_prediction_result` topic
  - **Customer**: From **LDES** (via **DWH**)
- NPL runs **risk model**
- `NPL → Flink (Consumer) → Kafka → npl_prediction_result`

---

### 5. Loan Decision
- **LDES** consumes `npl_prediction_result`
- LDES evaluates all inputs
- LDES makes **final decision**
- `LDES → Flink (Producer) → Kafka → loan_decision_result_notification`
- `LDES → Flink (Producer) → Kafka → loan_decision_result_disbursement`

---

### 6. Notification
- **NS** consumes `loan_decision_result_notification`
- NS sends **SMS/Email/Push** notification
- `NS → Flink (Consumer) → Kafka → loan_decision_result_notification`

---

### 7. Disbursement
- **LDIS** consumes `loan_decision_result_disbursement`
  - **Approved**: LDIS calls **payment API**
  - **Rejected**: No action
- `LDIS → Flink (Consumer) → Kafka → loan_decision_result_disbursement`

---

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

## Prerequisites

| Component              | Description         |
|------------------------|---------------------|
| jetBrains IntelliJ IDEA   |  IDE   |
| Java 11+ runtime environment    | install for local runs   |
| Scala 2.12.10   | install for local runs    |
| Apache Flink 1.13.2 cluster   | install for local runs    |
| Apache Kafka Cluster   | runs on Docker, no installation needed   |
| PostgreSQL 42.2.2+     | runs on Docker, no installation needed   |
| InfluxDB for Metrics   | runs on Docker, no installation needed   |
| Grafana                | runs on Docker, no installation needed   |

## Installation

### Prerequisites Setup

1. **jetBrains IntelliJ IDEA**
    | Platform         | Download Link                                                                 |
    |------------------|--------------------------------------------------------------------------------|
    | **Mac (Apple Silicon)** | [Download](https://www.jetbrains.com/idea/download/?section=mac)             |
    | **Mac (Intel)**         | [Download](https://www.jetbrains.com/idea/download/download-thanks.html?platform=mac) |
    | **Windows**             | [Download](https://www.jetbrains.com/idea/download/?section=windows)         |
    | **Linux**               | [Download](https://www.jetbrains.com/idea/download/?section=linux)           |
   

3. **Install Java 8 or 11**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install openjdk-11-jdk
   
   # macOS
   brew install openjdk@11
   ```

4. **Install SBT**
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

5. **Install Apache Flink 1.13.2**
   ```bash
   wget https://archive.apache.org/dist/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.12.tgz
   tar -xzf flink-1.13.2-bin-scala_2.12.tgz
   cd flink-1.13.2
   ./bin/start-cluster.sh
   ```

### Project Setup

```bash
# Clone the repository (This repo is public)
git clone https://github.com/cemtulunay/scala-realtime-loan-allocation
cd scala-realtime-loan-allocation

# Download dependencies and compile
sbt update
sbt compile

# Run tests
sbt test

# Create fat JAR with all dependencies (This is a later stage. First better run on the IDE)
sbt assembly

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

