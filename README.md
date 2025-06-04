# Real-Time Loan Processing System

A microservices-based loan processing system that leverages event-driven architecture with Kafka for asynchronous communication and constrained random data generation and randomly enriching data simulating "AI models" for decision making.


# Overview

This system processes loan applications through a series of microservices that evaluate income prediction (realtime for prospect customers, batch for customers), non-performing loan (NPL) risk, and ultimately make loan decisions. The architecture uses both synchronous API calls (API calls are imaginary) and asynchronous event-driven patterns to ensure efficient and scalable loan processing.

* MicorServices are imaginary
* Data is generated radomly (constrained random data generation)
* Data enriched in microservices randomly (simulating AI as decision maker)
* Data produced and consumed by Flink and flow between the Microservices by Kafka Topics
* Finally after Data turn back to LDES ("Loan Decision Service") randomly (constrained random data enrichment) Loan Decision made with the inputs from income prediction then npl service
* "Loan Disbursement Service" actually receives the final data from LDES and use it for time series analysis with real-time aggregations on a Grafana dashboard powered by influxDB
* "Loan Notification Service" works with final data similar as disbursement service. Provides realtime notifications and alerts

### External Systems (This part is imaginary)

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

### Event-Driven Architecture

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


### Data Flow

##### 1. Application Entry
- User submits loan application (Mobile/Web/Branch)
- **LAS** receives request
- LAS calls **DWH API** for demographics
- LAS sends sync request to **LDES**

##### 2. Customer Type Check
- **LDES** checks if applicant is an existing customer
  - **Customer**: Uses pre-calculated income prediction from **DWH**
  - **Prospect**: Triggers real-time income prediction

##### 3. Income Prediction (Prospects Only)
- `LDES → Flink (Producer) → Kafka → income_prediction_request`
- **IPS** consumes event
- IPS runs **AI model** (imaginary, just enriches data randomly)
- `IPS → Flink (Consumer) → Kafka → income_prediction_result`

##### 4. NPL Risk Assessment
- `LDES → Flink (Producer) → Kafka → npl_prediction_request`
- **NPL** consumes request
- NPL retrieves income data:
  - **Prospect**: From `income_prediction_result` topic
  - **Customer**: From **LDES** (via **DWH**)
- NPL runs **risk model**
- `NPL → Flink (Consumer) → Kafka → npl_prediction_result`

##### 5. Loan Decision
- **LDES** consumes `npl_prediction_result`
- LDES evaluates all inputs
- LDES makes **final decision**
- `LDES → Flink (Producer) → Kafka → loan_decision_result_notification`
- `LDES → Flink (Producer) → Kafka → loan_decision_result_disbursement`

##### 6. Notification
- **NS** consumes `loan_decision_result_notification`
- NS sends **SMS/Email/Push** notification
- `NS → Flink (Consumer) → Kafka → loan_decision_result_notification`

##### 7. Disbursement
- **LDIS** consumes `loan_decision_result_disbursement`
  - **Approved**: LDIS calls **payment API**
  - **Rejected**: No action
- `LDIS → Flink (Consumer) → Kafka → loan_decision_result_disbursement`


### Technology Stack

#### Core Technologies
- **Language**: Scala 2.12.10
- **Stream Processing**: Apache Flink 1.13.2
- **Message Broker**: Apache Kafka
- **Serialization**: Apache Avro 1.10.2
- **JSON Processing**: json4s-jackson 4.0.6

#### Databases
- **PostgreSQL** 42.2.2 - Primary relational database
- **InfluxDB** - Time-series data for metrics and monitoring

#### Analytics & Monitoring
- **Grafana** - Real-time dashboards via InfluxDB integration

### Prerequisites

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


# Installation

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

3. **Install Scala**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install scala

   # macOS  
   brew install scala@2.12
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

# Development

### Generic Classes

#### Generator
- `incomePredictionRequestDataGen`: This class generates synthetic loan prediction request data for testing Apache Flink streaming application.
  - Emits events with sequential timestamps and watermarks
  - Allows configurable delays between events
  - Supports parallel execution in Flink

#### Serializer
- `GenericAvroDeserializer`: This class deserializes Avro-encoded messages from Kafka into GenericRecord objects for Flink processing. Every event consumer uses that class.
  - Enables schema-based validation during deserialization
    - Kafka stores data as bytes, Flink needs structured objects
    - Avro provides schema evolution and compact binary format
    - Generic approach works with any Avro schema without creating specific classes
- `GenericAvroSerializer`: This class serializes Flink objects into Avro-encoded bytes for Kafka publishing for the same reasons with GenericAvroDeserializer class. Every event producer uses that class.
- `GenericRecordKryoSerializer`: For 2 criucial reasons Kryo internal serializer is used. Every event consumer uses that class.
  - `Flink State Management`: Enables Avro GenericRecords to be stored in Flink's stateful operations (keyed state, windows, checkpoints) - without this, you can't maintain state for Avro records.
  - `Schema Preservation`: Maintains the complete Avro schema during serialization, ensuring records can be correctly reconstructed after checkpointing/recovery - critical for fault tolerance.

#### Source
- `AbstractPostgreSQLToKafkaProducer`: Low-level component that handles the actual database polling, connection management, and record extraction - the "how" of reading from PostgreSQL. 
- `GenericPostgreSQLSource`: High-level orchestrator that wires together the source, serializer, and Kafka sink into a complete Flink job - the "what" of the entire pipeline.
  - Reuse the PostgreSQL source with different sinks (not just Kafka)
  - Swap implementations without changing the pipeline structure (GenericMySQLSource, GenericMongoDBSource, FileSource)
  - `AbstractPostgreSQLToKafkaProducer` and `GenericPostgreSQLSource` Used combined for producers reading data from postgreSQL DB.
- `StreamProducer`: Used for producers using data generator
  - Generic Type Conversion (S → T): Enables safe transformation from any source data format (S) to Kafka-ready format (T) without hardcoding - critical for handling different data types across your microservices (income predictions, NPL results, loan decisions)
  - Complete Pipeline Template: Provides the entire streaming infrastructure (source → transformation → state management → Kafka sink) as a reusable template - each microservice just implements the abstract methods instead of rebuilding the entire pipeline.

#### Target
- `AnlyticalStreamConsumer`: Used for Consumers using influxDB as a sink for real-time analytics
- `StreamConsumer`: Used for Consumers using postgreSQL as a sink

---

### Working Class Patterns

**Pattern 1 – Producer with random data generator**  
`incomePredictionRequestDataGen` → `predictionRequestGenerator` → `GenericAvroSerializer` → `StreamProducer` → `Kafka`

**Pattern 2 – Producer reading data from PostgreSQL**  
`PostgreSQL` → `GenericPostgreSQLSource` → `AbstractPostgreSQLToKafkaProducer` → `Kafka`

**Pattern 3 – Consumer sinks data to PostgreSQL**  
`Kafka` → `GenericRecordKryoSerializer` → `GenericAvroDeserializer` → `StreamConsumer` → `PostgreSQL`

**Pattern 4 – Analytical consumer, sinks data to InfluxDB**  
`Kafka` → `GenericRecordKryoSerializer` → `GenericAvroDeserializer` → `AnalyticalStreamConsumer` → `InfluxDB`

### Working Classes

#### loanDecisionService
- `event1` Producer: pattern1
- `event3` Producer: pattern1
- `event4` Consumer: pattern3
- `event6` Producer: pattern2
- **Execution:** `def main` runs all 4 pieces in parallel

#### incomePredictionService
- `event1` Consumer: pattern3
- `event2` Producer: pattern2
- **Execution:** `def main` runs all 2 pieces in parallel

#### nonPerformingLoanService
- `event2` Consumer: pattern3
- `event3` Consumer: pattern3
- `event4` Producer: pattern2
- **Execution:** `def main` runs all 3 pieces in parallel

#### loanDisbursementService
- `event2` Consumer: pattern4
- **Execution:** `def main` runs all 1 piece

#### utils
- Functions for Constrained Random Data Generation
- Loan Disbursement Service – Realtime Analytical Functions
  
---

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
