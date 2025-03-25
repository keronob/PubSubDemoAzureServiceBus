import json
import time
from azure.servicebus import ServiceBusClient

CONNECTION_STR = (
    "Endpoint=sb://kbservicebustest.servicebus.windows.net/;"
    "SharedAccessKeyName=kbservicebustest;"
    "SharedAccessKey=uZMAFQQ7buLEkePeip9IrnO2bPm+nFzFH+ASbPqO6XY=;"
    "EntityPath=kbtestque"
)
QUEUE_NAME = "kbtestque"

def receive_messages():
    with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR) as client:
        with client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5) as receiver:
            print("Listening for messages. Press Ctrl+C to exit.")
            while True:
                # Try to receive up to 1 message. Adjust max_message_count if you want more per batch.
                messages = receiver.receive_messages(max_message_count=1, max_wait_time=5)
                if not messages:
                    # No messages received in this interval, so pause briefly before trying again.
                    time.sleep(1)
                    continue

                for msg in messages:
                    # msg.body is often an iterable of bytes, so join them
                    message_bytes = b"".join(msg.body)
                    message_str = message_bytes.decode("utf-8")
                    try:
                        json_data = json.loads(message_str)
                        print("Received JSON:", json_data)
                    except json.JSONDecodeError as e:
                        print("Failed to decode JSON:", e)
                    # Mark the message as complete so it's removed from the queue.
                    receiver.complete_message(msg)

if __name__ == "__main__":
    receive_messages()
