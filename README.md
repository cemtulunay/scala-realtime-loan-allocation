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

# Development Details

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

---

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

6. **DBeaver (Database Management Tool)**
   This UI is for creating postgreSQL DB tables easily and querying the tables
   
   | Platform         | Download Link                                                                 |
   |------------------|--------------------------------------------------------------------------------|
   | **Mac**          | [Download](https://dbeaver.io/files/dbeaver-ce-latest-macos-x64.dmg)        |
   | **Windows**      | [Download](https://dbeaver.io/files/dbeaver-ce-latest-win32.win32.x86_64.zip) |
   | **Linux (deb)**  | [Download](https://dbeaver.io/files/dbeaver-ce_latest_amd64.deb)            |
   | **Linux (rpm)**  | [Download](https://dbeaver.io/files/dbeaver-ce-latest-stable.x86_64.rpm)    |

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

Make sure all the external libaraies and dependencies are downloaded and inplace.
Can be refreshed by clicking on the right hand side Hexagon icon to sbt control panel and recycle like refresh button can be clicked.
External libraries should be visible as in that image.

<img width="1773" alt="Screenshot 2025-06-03 at 11 36 58 PM" src="https://github.com/user-attachments/assets/5fb9a1e9-58a8-4d5c-aa49-66919217d438" />


# Local Run

## A-) Rise up the Infrsastructure

### 1-) Kafka
terminal1, terminal2 are must. rise up the Kafka on docker and create topics
terminal3 .. terminal7 are optional. These are console consumers. Can be useful in order to be  sure if there is data in a particular Kaka topic

```bash
// terminal1 - rise up the docker
cd IdeaProjects/scala-realtime-loan-allocation/docker/kafka-realtime-loan
docker-compose up

// terminal2 - docker bash - create topic
docker exec -it flink-broker bash

/bin/kafka-topics --bootstrap-server localhost:9092 --topic income_prediction_request --create
/bin/kafka-topics --bootstrap-server localhost:9092 --topic income_prediction_result --create
/bin/kafka-topics --bootstrap-server localhost:9092 --topic npl_prediction_request --create
/bin/kafka-topics --bootstrap-server localhost:9092 --topic npl_prediction_result --create
/bin/kafka-topics --bootstrap-server localhost:9092 --topic loan_decision_result_disbursement --create

// terminal3 - docker bash - create console consumer
docker exec -it flink-broker bash
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic income_prediction_request --from-beginning

// terminal4 - docker bash - create console producer
docker exec -it flink-broker bash
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic income_prediction_result --from-beginning

// terminal5 - docker bash - create console producer
docker exec -it flink-broker bash
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic npl_prediction_request --from-beginning

// terminal6 - docker bash - create console producer
docker exec -it flink-broker bash
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic npl_prediction_result  --from-beginning

// terminal7 - docker bash - create console producer
docker exec -it flink-broker bash
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic npl_loan_decision_result_disbursement  --from-beginning
```

### 2-) JDBC - PostgreSQL
```bash
// terminal1 - rise up the docker
cd IdeaProjects/scala-realtime-loan-allocation/docker/postgres-realtime-loan
Docker-compose up

// terminal2 -docker bash-create DB
docker exec -it flink-postgres bash
psql -U docker

//create
create database loan_db;

// Below that part can be applied on a PL/SQL UI (for example DBeaver) 
```

DDL's for postgreSQL DB tables. The most recent ones and prepared SQL's can be found under project files
path = `/Users/cemtulunay/IdeaProjects/scala-realtime-loan-allocation/docker/create-scripts/create_scripts.txt`

The ones are on top are the most recents above a date seperator. 
For different versions there may be different versions of scripts needed, can be found below.

Coonnection information for loan_db is as seen on the image

```bash
user: docker
pass: docker

```

<img width="1046" alt="Screenshot 2025-06-03 at 11 31 19 PM" src="https://github.com/user-attachments/assets/8ba6d82e-ad62-496c-9701-82d2c9a74fe6" />

TABLE-1
```bash
-- public.income_prediction definition
-- Drop table
-- DROP TABLE public.income_prediction;

CREATE TABLE public.income_prediction (
request_id varchar(255) NOT NULL,
application_id varchar(255) NOT NULL,
customer_id int4 NOT NULL,
prospect_id int4 NOT NULL,
income_requested_at int8 NOT NULL,
system_time int8 NOT NULL,
e1_produced_at int8 NOT NULL,
income_source varchar(255) NULL,
is_customer bool NULL,
kafka_source varchar(255) NULL,
predicted_income float8 NULL,
e1_consumed_at int8 NULL,
sent_to_npl bool DEFAULT false NULL,
e2_produced_at int8 NULL,
CONSTRAINT income_prediction_pkey PRIMARY KEY (request_id)
);
CREATE INDEX idx_income_not_sent1 ON public.income_prediction USING btree (sent_to_npl) WHERE (sent_to_npl = false);
```

TABLE-2
```bash
-- public.npl_prediction definition
-- Drop table
-- DROP TABLE public.npl_prediction;

CREATE TABLE public.npl_prediction (
request_id varchar(255) NOT NULL,
application_id varchar(255) NOT NULL,
customer_id int4 NOT NULL,
prospect_id int4 NOT NULL,
income_requested_at int8 NULL,
system_time int8 NOT NULL,
e1_produced_at int8 NULL,
income_source varchar(255) NULL,
is_customer bool NULL,
kafka_source varchar(255) NULL,
predicted_income float8 NULL,
e1_consumed_at int8 NULL,
sent_to_npl bool NULL,
e2_produced_at int8 NULL,
e2_consumed_at int8 NULL,
npl_requested_at int8 NULL,
e3_produced_at int8 NULL,
e3_consumed_at int8 NULL,
credit_score  int8 NOT NULL,
sent_to_ldes bool DEFAULT false NULL,
e4_produced_at int8 NULL,
predicted_npl float8 NOT NULL,
CONSTRAINT npl_prediction_pkey PRIMARY KEY (request_id)
);
CREATE INDEX idx_not_sent_to_ldes1 ON public.npl_prediction USING btree (sent_to_ldes) WHERE (sent_to_ldes = false);
```

TABLE-3
```bash
-- public.loan_result definition
-- Drop table
-- DROP TABLE public.loan_result;

CREATE TABLE public.loan_result (
request_id varchar(255) NOT NULL,
application_id varchar(255) NOT NULL,
customer_id int4 NOT NULL,
prospect_id int4 NOT NULL,
income_requested_at int8 NULL,
system_time int8 NOT NULL,
e1_produced_at int8 NULL,
income_source varchar(255) NULL,
is_customer bool NULL,
kafka_source varchar(255) NULL,
predicted_income float8 NULL,
e1_consumed_at int8 NULL,
sent_to_npl bool NULL,
e2_produced_at int8 NULL,
e2_consumed_at int8 NULL,
npl_requested_at int8 NULL,
e3_produced_at int8 NULL,
e3_consumed_at int8 NULL,
credit_score  int8 NOT NULL,
sent_to_ldes bool NULL,
e4_produced_at int8 NULL,
predicted_npl float8 NOT NULL,
e4_consumed_at int8 NULL,
debt_to_income_ratio  float8 NOT NULL,
employment_industry varchar(255) NULL,
employment_stability  float8 NOT NULL,
loan_decision bool NOT NULL,
loan_amount float8 NOT NULL,
sent_to_ns bool DEFAULT false NULL,
e5_produced_at int8 NULL,
sent_to_ldis bool DEFAULT false NULL,
e6_produced_at int8 NULL,
CONSTRAINT loan_result_pkey PRIMARY KEY (request_id)
);
CREATE INDEX idx_not_sent_to_ldis1 ON public.loan_result USING btree (sent_to_ldis) WHERE (sent_to_ldis = false);
CREATE INDEX idx_not_sent_to_ns1 ON public.loan_result USING btree (sent_to_ns) WHERE (sent_to_ns = false);
```

### 3-) Analytics - (InfluxDB + Grafana)

This will rise up both influxDB + Grafana

```bash
cd IdeaProjects/scala-realtime-loan-allocation/docker/analytics-realtime-loan
docker-compose up
```

#### 3.1-) InfluxDB
Once influxDB up and running on docker. influxDB UI can be reached out through browser on localhost.
** No need to create bucket or any data field. It is created automatically on InfluxDB. **

UI: ```http://localhost:8086```

under explorer section, there are 2 options for querying
i-) Script Editor: You can directly write your own query. Exanples can be seen below.
<img width="1672" alt="Screenshot 2025-06-03 at 11 55 16 PM" src="https://github.com/user-attachments/assets/5a786853-1454-4fb6-8023-52ffc192a23e" />

ii-) Query Builder: Create query from UI.
<img width="1676" alt="Screenshot 2025-06-03 at 11 55 44 PM" src="https://github.com/user-attachments/assets/f0492cfc-8773-43f1-87de-2623c279f393" />


```bash
InfluxDB
Username: admin
Password: admin123
Organization: loan-org
bucket: loan-analytics
admin_token: loan-analytics-token
```

This query shows all the data points for every field. Aggregates for every 2 seconds.
```bash
from(bucket: "loan-analytics")
 |> range(start: -1h)
 |> filter(fn: (r) => r._measurement == "loan_decisions")
 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
```

![image](https://github.com/user-attachments/assets/bf8b2fdc-476e-47e6-9288-0cb9542a3324)

This query returns only approved_rate field
```bash
from(bucket: "loan-analytics") 
 |> range(start: -1h) 
 |> filter(fn: (r) => r._measurement == "loan_decisions")
```

![image](https://github.com/user-attachments/assets/e5d8fb4e-4684-4eb4-88da-3eb438f82dfe)


#### 3.2-) Grafana
Once Grafana up and running on docker. Grafana UI can be reached out through browser on localhost.

UI: ```http://localhost:3000/```

#### i-) create a Data Source for the Dashboard
click under the connections "add a new connection" and search for inlfuxDB
<img width="3004" alt="Screenshot 2025-06-04 at 12 07 46 PM" src="https://github.com/user-attachments/assets/0fa6141b-899b-4fac-8306-87f42d30f1da" />

click on "add a new Datasource"
<img width="2272" alt="Screenshot 2025-06-04 at 12 08 37 PM" src="https://github.com/user-attachments/assets/891395f7-9a25-4907-965d-6088c93604ec" />

In this project influxDB version 2 is used so for Query Language Flux should be selected
<img width="1135" alt="Screenshot 2025-06-04 at 12 09 05 PM" src="https://github.com/user-attachments/assets/25223a69-2f73-480f-ab76-767791759d8b" />

These are criterias for that project. You can always change them but for example Default Bucket  should be changed in influxDB so in the code for "loan disbursement Service" as well
<img width="1843" alt="Screenshot 2025-06-04 at 12 10 41 PM" src="https://github.com/user-attachments/assets/4e0c5866-5320-4c49-b549-d268542a86f5" />

#### ii-) create a new Dashboard or import a Dashboard by using the Data Source already created

You can either create a new dashboard by clicking "new" here or open an existing one
<img width="3002" alt="Screenshot 2025-06-04 at 12 05 59 PM" src="https://github.com/user-attachments/assets/3157dd06-2283-41a7-b8a7-1225b71d935c" />

If you click on "new" There will be options in order to create a new one or import an existing one.
<img width="2224" alt="Screenshot 2025-06-04 at 12 13 21 PM" src="https://github.com/user-attachments/assets/a5d2c3da-d693-4149-8913-6c5ecb56f94b" />

You can simply import the dashboard .json file that already shared with the project or you can create another Dashoard from scratch with the bunch of analytical fields already provided on influxDB
```../scala-realtime-loan-allocation/docker/analytics-realtime-loan/grafana/dashboards/loan-analytics.json```

If you import the existing dashboard. You may expect to see a dashboard as below.
<img width="3003" alt="Screenshot 2025-06-04 at 12 04 43 PM" src="https://github.com/user-attachments/assets/8b78fbe3-1be0-4e46-bbd9-8b2d5be92657" />


## B-) Run Locally

Running locally means here, without deploying flink into docker. The rest of the infrastructure will run on the docker, thats why we 1st rose up the infrastructure for the rest.
Now the environment is ready. Once you can run the system locally then we can create fat jar file for the flink project and deploy it into docker and run on docker completely.
When we run locally it will be faster to make changes and observe differences. Once we are sure about the version of the code we can create fat file and deploy it to docker.

As seen here and in the same order you can simply run the "def main" functions and data will start to flow and you can see realtime analytics on influxDB UI and Grafana dashboard
Thats the order for the local run

- loanDecisionService
- incomePredictionService
- nonPerformingLoanService
- loanDisbursementService
  
<img width="2542" alt="Screenshot 2025-06-04 at 1 06 59 PM" src="https://github.com/user-attachments/assets/04672c39-d987-4a44-8056-515301ccb8db" />


# Run on Docker
to be continued...
