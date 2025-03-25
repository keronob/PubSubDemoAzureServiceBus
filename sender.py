from flask import Flask, request, jsonify
import json
from azure.servicebus import ServiceBusClient, ServiceBusMessage

app = Flask(__name__)

CONNECTION_STR = (
    "Endpoint=sb://kbservicebustest.servicebus.windows.net/;"
    "SharedAccessKeyName=kbservicebustest;"
    "SharedAccessKey=uZMAFQQ7buLEkePeip9IrnO2bPm+nFzFH+ASbPqO6XY=;"
    "EntityPath=kbtestque"
)
QUEUE_NAME = "kbtestque"

@app.route('/send', methods=['POST'])
def send_message_api():
    # Try to get JSON payload from the request. Use default if not provided.
    data = request.get_json(silent=True)
    if not data:
        data = {
            "message": "Message Queue Body static",
            "timestamp": "2025-03-25T12:00:00Z"
        }

    # Connect to Azure Service Bus and send the message.
    with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR) as client:
        sender = client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            # Convert the payload to a JSON string.
            json_message = json.dumps(data)
            message = ServiceBusMessage(json_message, content_type="application/json")
            sender.send_messages(message)
            print("Message sent!")

    return jsonify({"status": "Message sent!", "payload": data}), 200


@app.route('/receive', methods=['GET'])
def receive_messages_api():
    messages_data = []
    # Create a ServiceBusClient instance.
    with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR) as client:
        # Use a receiver with a max_wait_time of 5 seconds.
        with client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5) as receiver:
            # Attempt to receive up to 10 messages in one call.
            messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)
            for msg in messages:
                # Combine message parts (msg.body might be an iterable of bytes).
                message_bytes = b"".join(msg.body)
                message_str = message_bytes.decode("utf-8")
                try:
                    # Parse the message as JSON.
                    json_data = json.loads(message_str)
                except json.JSONDecodeError:
                    json_data = {"error": "Failed to decode JSON", "raw": message_str}
                messages_data.append(json_data)
                # Complete the message so it is removed from the queue.
                receiver.complete_message(msg)
    return jsonify({"messages": messages_data}), 200

if __name__ == "__main__":
    # Run the Flask app on port 5000.
    app.run(debug=True, host="0.0.0.0", port=5000)
