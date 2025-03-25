import pika
import ssl

def main():
    try:
        # Use SSL context for secure connection
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Configure connection parameters with SSL
        connection_params = pika.URLParameters("amqps://cd2gzegs:eHB0sWluUm1fUwvE0fTcd7w7s4qH1uWE@toucan.lmq.cloudamqp.com/cd2gzegs")
        connection_params.ssl_options = pika.SSLOptions(ssl_context)

        # Establish connection
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        queue_name = "test_queue"
        channel.queue_declare(queue=queue_name, durable=True)

        message = "Hello from Python publisher!"
        channel.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f"[x] Sent '{message}'")

        connection.close()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Connection error: {e}")
    except pika.exceptions.AMQPChannelError as e:
        print(f"Channel error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()