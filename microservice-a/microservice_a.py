import boto3
import json
import uuid
import time
import threading
import atexit
import os

# Initialize AWS clients
sqs = boto3.client('sqs', region_name='us-east-1')

# Define queue names and existing Request Queue URL
request_queue_url = 'https://sqs.us-east-1.amazonaws.com/173363942029/request-queue'
response_queue_base_name = 'response-queue'

# Ensure the directory exists
log_dir = "/app/logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Use the full path for the log file
log_file_path = os.path.join(log_dir, "thread_logs.txt")

# Thread-local storage
thread_local = threading.local()

# Dictionary to track requests
response_handlers = {}

# Lock for writing to the file
file_lock = threading.Lock()

# Thread counter for naming
thread_counter = 1
thread_counter_lock = threading.Lock()

# List of names to send as payloads
names = ["Alon", "Dan", "Omer"]

# Create or get the Response SQS queue
def create_response_queue(base_name):
    unique_id = str(uuid.uuid4())
    queue_name = f"{base_name}-{unique_id}"
    response = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            'DelaySeconds': '0',
            'MessageRetentionPeriod': '86400'  # 1 day
        }
    )
    return response['QueueUrl'], queue_name

# Get the Queue URL and ARN
def get_response_queue_url_and_arn(base_name):
    queue_url, queue_name = create_response_queue(base_name)
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['QueueArn']
    )['Attributes']['QueueArn']
    return queue_url, queue_arn, queue_name

# Initialize the Response Queue
response_queue_url, response_queue_arn, response_queue_name = get_response_queue_url_and_arn(response_queue_base_name)

# Function to send a message to the Request Queue
def send_request_message(name):
    request_id = str(uuid.uuid4())
    message_body = {
        'requestId': request_id,
        'responseQueueArn': response_queue_arn,
        'payload': name
    }

    # Store the request_id in thread-local storage
    thread_local.request_id = request_id

    # Store the response handler in the dictionary
    response_handlers[request_id] = threading.current_thread().name

    sqs.send_message(
        QueueUrl=request_queue_url,
        MessageBody=json.dumps(message_body)
    )

    # Log the request without the 'responseQueueArn' field
    log_request(name, {key: value for key, value in message_body.items() if key != 'responseQueueArn'})

    print(f"Sent request with payload: {name}")

def log_request(name, data):
    with file_lock:
        with open(log_file_path, "a") as f:
            f.write(f"Thread {threading.current_thread().name} - Sent payload: {name} - JSON: {json.dumps(data)}\n")

# Function to poll messages from the Response Queue
def poll_response_queue():
    while True:
        response = sqs.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        if 'Messages' in response:
            for message in response['Messages']:
                message_body = json.loads(message['Body'])
                print(f"Processing acknowledgment for request ID: {message_body['requestId']}")
                # Simulate processing time
                time.sleep(5)  # Simulate processing for 5 seconds
                # Handle the response
                handle_response(message_body)
                # Delete the message after processing
                sqs.delete_message(
                    QueueUrl=response_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
            print("No messages received")

def handle_response(message_body):
    # Get the request_id from the message body
    request_id = message_body['requestId']

    # Find the correct thread to handle the response
    response_thread_name = response_handlers.get(request_id)

    if response_thread_name:
        # Process and log the response
        processed_response = process_response(message_body)
        log_response(response_thread_name, processed_response)

        # Remove the handler from the dictionary
        del response_handlers[request_id]

def process_response(response_data):
    # Process the response data to make it readable (e.g., extract relevant fields)
    return {
        "data": response_data.get("payload")
    }

def log_response(thread_name, data):
    with file_lock:
        with open(log_file_path, "a") as f:
            f.write(f"Thread {thread_name} - Received payload: {json.dumps(data)}\n")

# Function to delete the response queue
def delete_response_queue():
    sqs.delete_queue(QueueUrl=response_queue_url)
    print(f"Deleted response queue: {response_queue_name}")

# Register the cleanup function to run on exit
atexit.register(delete_response_queue)

# Main function
if __name__ == "__main__":
    print(f"Response Queue Name: {response_queue_name}")
    # Start a thread to poll the response queue
    polling_thread = threading.Thread(target=poll_response_queue)
    polling_thread.daemon = True
    polling_thread.start()

    # Send request messages every 5 seconds
    try:
        name_index = 0
        while True:
            with thread_counter_lock:
                thread_name = f"Thread-{thread_counter}"
                thread_counter += 1
            name = names[name_index % len(names)]
            name_index += 1
            thread = threading.Thread(target=send_request_message, args=(name,), name=thread_name)
            thread.start()
            thread.join()
            time.sleep(5)  # Send a request every 5 seconds
    except (KeyboardInterrupt, SystemExit):
        print("Microservice A is shutting down...")
        delete_response_queue()
