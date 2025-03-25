import pika

AMQP_URL = "amqps://cd2gzegs:eHB0sWluUm1fUwvE0fTcd7w7s4qH1uWE@toucan.rmq.cloudamqp.com/cd2gzegs"


def main():
    connection_params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    queue_name = "test_queue"
    channel.queue_declare(queue=queue_name, durable=True)

    # Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        print(f"[x] Received: {body.decode()}")
        # If you want to manually acknowledge, you'd use:
        # ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True  # Automatically acknowledge messages
    )

    print("[*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    main()
