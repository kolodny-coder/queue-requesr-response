Queuing Request Response App

This repository contains two microservices, Service A and Service B, that demonstrate a simple queuing request-response mechanism using Amazon SQS.
Building the Containers
Service A

To build the Docker container for Service A, run the following command:

```sh
docker build -t microservice-a .
```
Service B

To build the Docker container for Service B, run the following command:

```sh
docker build -t microservice-b .
````

Running the Containers
Service A

To run the Docker container for Service A, use the following command:

```sh

docker run -e ISENGARD_PRODUCTION_ACCOUNT=false \
           -e AWS_ACCESS_KEY_ID=<your_aws_access_key_id> \
           -e AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key> \
           -e AWS_SESSION_TOKEN=<your_aws_session_token> \
           -v /path/to/logs:/app/logs \
           microservice-a
````
Replace <your_aws_access_key_id>, <your_aws_secret_access_key>, and <your_aws_session_token> with your actual AWS credentials. Also, replace /path/to/logs with the actual path to the logs directory on your host machine.
Service B

To run the Docker container for Service B, use the following command:

```sh

docker run -e ISENGARD_PRODUCTION_ACCOUNT=false \
           -e AWS_ACCESS_KEY_ID=<your_aws_access_key_id> \
           -e AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key> \
           -e AWS_SESSION_TOKEN=<your_aws_session_token> \
           -v /path/to/logs:/app/logs \
           microservice-b
````
Replace <your_aws_access_key_id>, <your_aws_secret_access_key>, and <your_aws_session_token> with your actual AWS credentials. Also, replace /path/to/logs with the actual path to the logs directory on your host machine.
Overview
Service A

Service A sends request messages to an SQS request queue. Each request contains a name payload and a reference to a response queue where Service B should send the response. Service A also logs each request to a file without including sensitive information such as the responseQueueArn.
Service B

Service B polls the SQS request queue, processes each request by looking up the corresponding credit card number for the provided name, and sends the response to the specified response queue. It then logs each response.
Logging

Logs for both services are written to the specified host directory that is mounted to /app/logs within the containers. Make sure the specified directory exists and is writable.
Cleanup

Both services are designed to delete their respective response queues upon shutdown to ensure no orphaned queues are left behind.