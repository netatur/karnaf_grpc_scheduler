### Microservices Callback Scheduler

![karnaf_grpc_scheduler Diagram.drawio.png](karnaf_grpc_scheduler%20Diagram.drawio.png)

### Components:

### 1. **REST API – `rest_api`**

Receives incoming HTTP `POST` requests to register a scheduled callback.
**Request body parameters:**

- `id` – Unique identifier for the request
- `url_callback` – URL to send the callback to
- `time` – Delay in seconds before the callback is triggered

* Simple ETL.

### 2. **gRPC Client – `grpc_client`**

Consumes `CallbackRequest` messages from the REST API and sends them via gRPC.

### 3. **gRPC Server – `grpc_server`**

Receives gRPC messages and publishes them to **RabbitMQ** with a delay.

### 4. **Scheduler Service – `scheduler_service`**

Consumes delayed messages from RabbitMQ and sends the actual callback to the provided URL.

## Important Features in RabbitMQ !

1. **ack/nack** – Indicates whether a message was successfully processed or not, and if not, whether it should be
   requeued.

2. **retry** – A repeated attempt to deliver a message to a consumer after it failed to process it.

3. **time-delay** – A feature that allows delaying the delivery of a message to the consumer for a specified period of
   time.

4. **dead-letter-queue (DLQ)** – A special queue where messages are routed when they cannot be processed successfully —
   such as after being rejected, expired, or exceeding the maximum number of delivery attempts.

## How to run the project:

### Prerequisites

- Python 3.10+
- uv package manager
- RabbitMQ running in Docker
  > cd .\hack
  > docker build -t rabbitmq-3.13.7-delayed .
  > docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq-3.13.7-delayed

> .\run_all.bat (I wanted to wrap the project in Docker, but due to lack of time, I'm running it using a script)

- test the project:
  > uv run .\hack\send_request.py

## Notes: 
1. The project supports auto-scaling.
2. I would also add Docker, monitoring, and handling of specific exceptions.
3. I would like to store all incoming requests in DB for backup purposes.
4. I didn’t fix all the mypy issues - it’s just a POC anyway. 🙂
5. I would implement automatic export to the DLQ, or add an alert for it.
