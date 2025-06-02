# Loan Processing System

A microservices-based loan processing system that leverages event-driven architecture with Kafka for asynchronous communication and AI models for decision making.

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
- **AI-Powered Decision Making**: Leverages machine learning models for income and NPL predictions
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

```bash
# Clone the repository
git clone https://github.com/your-org/loan-processing-system.git

# Navigate to project directory
cd loan-processing-system

# Build with SBT
sbt clean compile

# Run tests
sbt test

# Create assembly JAR
sbt assembly

# Deploy services (example using Docker)
docker-compose up -d
```

## Build Configuration

The project uses SBT (Scala Build Tool) with the following key dependencies:

```scala
scalaVersion := "2.12.10"
flinkVersion := "1.13.2"
```

### Main Dependencies
- **Apache Flink**: Stream processing framework
- **Kafka Connector**: For event-driven architecture
- **PostgreSQL & Cassandra**: Database connectivity
- **InfluxDB Client**: Time-series metrics
- **Elasticsearch**: Log aggregation
- **Apache Commons Math**: Statistical analysis
- **OpenCSV**: Data export capabilities

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

## Monitoring

### Real-time Dashboards
- **Grafana**: Connect to InfluxDB for real-time metrics visualization
  - Loan processing rates
  - Decision latency metrics
  - Service health indicators
  - Resource utilization

### Log Analysis
- **Kibana**: Connected to Elasticsearch for:
  - Error tracking and debugging
  - Audit trail analysis
  - Performance profiling

### Service Endpoints
- Service health: `/health`
- Metrics endpoint: `/metrics`
- Flink job status: `http://flink-jobmanager:8081`

### Key Metrics
- Kafka consumer lag per topic
- Event processing latency
- Model prediction response times
- Database connection pool status
- Loan approval/rejection rates

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the [LICENSE_TYPE] License - see the [LICENSE.md](LICENSE.md) file for details.
