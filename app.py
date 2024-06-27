import streamlit as st
import boto3
import pandas as pd
import time

# Initialize AWS clients
sqs = boto3.client('sqs', region_name='us-east-1')

# Define the request queue URL
request_queue_url = 'https://sqs.us-east-1.amazonaws.com/173363942029/request-queue'
response_queue_prefix = 'response-queue'


# Function to get the number of messages in a queue
def get_queue_message_count(queue_url):
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(attributes['Attributes']['ApproximateNumberOfMessages'])


# Function to list all queues
def list_all_queues():
    response = sqs.list_queues()
    return response.get('QueueUrls', [])


# Streamlit app layout
st.title("AWS SQS Queue Monitoring")
st.markdown("### Request Queue")
st.text("Monitoring the request queue and its associated response queues.")

# Display the request queue message count
request_message_count = get_queue_message_count(request_queue_url)
request_metric = st.metric(label="Request Queue", value=request_message_count)

# List all queues and filter the response queues
all_queues = list_all_queues()
response_queue_urls = [q for q in all_queues if response_queue_prefix in q]

# Display the response queues' message counts
response_data = []
for queue_url in response_queue_urls:
    queue_name = queue_url.split('/')[-1]
    message_count = get_queue_message_count(queue_url)
    response_data.append({'Queue Name': queue_name, 'Messages': message_count})

# Create a DataFrame and display it
df = pd.DataFrame(response_data)
response_table = st.empty()
response_table.dataframe(df)

# Continuously update the metrics
st.text("Updating every 5 seconds...")
while True:
    # Update request queue metric
    request_message_count = get_queue_message_count(request_queue_url)
    request_metric.metric(label="Request Queue", value=request_message_count)

    # Update response queues metrics
    response_data = []
    response_queue_urls = [q for q in all_queues if response_queue_prefix in q]
    for queue_url in response_queue_urls:
        queue_name = queue_url.split('/')[-1]
        message_count = get_queue_message_count(queue_url)
        response_data.append({'Queue Name': queue_name, 'Messages': message_count})

    df = pd.DataFrame(response_data)
    response_table.dataframe(df)

    time.sleep(5)
