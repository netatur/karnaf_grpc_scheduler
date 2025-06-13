# RABBIT
REQUEST_QUEUE = "requests"
REQUEST_DLQ_QUEUE = "requests_dlq"
REQUEST_DLX_EXCHANGE = "requests_dlq_exchange"
REQUEST_RETRY_EXCHANGE = "requests_retry_exchange"
REQUEST_DELAYED_EXCHANGE = "requests_delayed_exchange"
REQUEST_DELAYED_ROUTING_KEY = "requests_delayed_key"

SCHEDULER_QUEUE = "scheduler"
SCHEDULER_DLQ_QUEUE = "scheduler_dlq"
SCHEDULER_RETRY_QUEUE = "scheduler_retry"
SCHEDULER_RETRY_EXCHANGE = "scheduler_retry_exchange"
SCHEDULER_DLX_EXCHANGE = "scheduler_dlq_exchange"
SCHEDULER_DELAYED_EXCHANGE = "scheduler_delayed_exchange"
SCHEDULER_DELAYED_ROUTING_KEY = "scheduler_delayed_key"


RETRY_DELAY_MS = 5000  # 5 seconds delay for retries
MAX_RETRIES = 5

# GRPS
GRPC_HOST = "localhost"
GRPC_PORT = 50051

# FASTAPI
API_HOST = "localhost"
API_PORT = 8081
