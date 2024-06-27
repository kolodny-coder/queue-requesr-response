import boto3
import json
import time
from botocore.exceptions import ClientError

# Initialize AWS clients
sqs = boto3.client('sqs', region_name='us-east-1')

# Define queue names and existing Request Queue URL
request_queue_url = 'https://sqs.us-east-1.amazonaws.com/173363942029/request-queue'

# Sample data
data = [
    {"name": "Alon", "creditCardNo": "123"},
    {"name": "Dan", "creditCardNo": "456"},
    {"name": "Omer", "creditCardNo": "789"}
]

# Function to get credit card number by name
def get_credit_card_info(name):
    for entry in data:
        if entry["name"] == name:
            return entry["creditCardNo"]
    return None

# Function to poll messages from the Request Queue and send acknowledgments to the Response Queue
def process_messages():
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=request_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                print("The request queue does not exist.")
                break
            else:
                print(f"An error occurred: {e}")
                continue

        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    message_body = json.loads(message['Body'])
                    request_id = message_body['requestId']
                    response_queue_arn = message_body['responseQueueArn']
                    name = message_body['payload']

                    try:
                        response_queue_url = sqs.get_queue_url(QueueName=response_queue_arn.split(':')[-1])['QueueUrl']
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                            print(f"The response queue {response_queue_arn.split(':')[-1]} does not exist.")
                            continue
                        else:
                            print(f"An error occurred: {e}")
                            continue

                    # Get credit card info
                    credit_card_info = get_credit_card_info(name)
                    if credit_card_info is None:
                        print(f"No credit card info found for {name}")
                        continue

                    # Simulate processing time
                    print(f"Processing request ID: {request_id} for name: {name}")
                    time.sleep(5)  # Simulate processing for 5 seconds

                    # Send acknowledgment message to the Response Queue
                    ack_message_body = {
                        'requestId': request_id,
                        'payload': {
                            'name': name,
                            'creditCardNo': credit_card_info
                        }
                    }
                    sqs.send_message(
                        QueueUrl=response_queue_url,
                        MessageBody=json.dumps(ack_message_body)
                    )
                    print(f"Sent acknowledgment for request ID: {request_id} with credit card info for {name}")

                    # Delete the message from the Request Queue after processing
                    sqs.delete_message(
                        QueueUrl=request_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    print(f"An error occurred while processing message: {e}")
                    continue
        else:
            print("No messages received")

if __name__ == "__main__":
    process_messages()
