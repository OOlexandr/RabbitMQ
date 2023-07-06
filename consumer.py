import pika
import sys
from bson import ObjectId
import model
import connect


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='messages_queue', durable=True)

    def callback(ch, method, properties, body):
        id = body.decode()
        message = model.Messages.objects(id=id)
        message.update(processed=True)
        print(f" [x] Received {message.get().content}")

    channel.basic_consume(queue='messages_queue', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)