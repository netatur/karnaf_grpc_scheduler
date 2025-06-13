![karnaf_grpc_scheduler Diagram.drawio.png](karnaf_grpc_scheduler%20Diagram.drawio.png)

Components: 
1. rest-api: 
    Get post request (/register) with parameters: 
    1. id 
    2. url_callback
    3. time
   * Simple ETL.
2. grpc-client: 
    consume CallbackRequest and send using grpc.
3. grpc-server:
    get grpc message and publish to rabbitmq with time-delay
4. scheduler-service:
    consume after time-delay and send the callback.

Important features in Rabbitmq!
1. ack/nack - indicates whether a message was successfully processed or not, and if not, whether it should be requeued.
2. retry - A repeated attempt to deliver a message to a consumer after it failed to process it.
3. time-delay - feature that allows delaying the delivery of a message to the consumer for a specified period of time.
4. dead-letter-queue - A special queue where messages are routed when they cannot be processed successfully—such as after being rejected, expired, or exceeding the maximum number of delivery attempts.

How to run the project: 
1. Make sure you have **uv** installed.
2. run rabbitmq: 
    > cd .\hack
    > docker build -t rabbitmq-3.13.7-delayed .
    > docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq-3.13.7-delayed
3. .\run_all.bat (I wanted to wrap the project in Docker, but due to lack of time, I'm running it using a script)
4. test the project: 
    > uv run .\hack\send_request.py

