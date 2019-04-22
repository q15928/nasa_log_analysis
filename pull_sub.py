import argparse
from google.cloud import pubsub_v1

def callback(message):
  print(('Received message: {}'.format(message)))
  message.ack()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pull data from Cloud Pub/Sub')
    parser.add_argument('--project', help='GCP project ID', default='jf-project-20190218')
    parser.add_argument('--subscription', help='Subscription name of the topic', default='nasa_log')
    args = parser.parse_args()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(args.project, args.subscription)

    future = subscriber.subscribe(subscription_path, callback=callback)

    # Blocks the thread while messages are coming in through the stream. Any
    # exceptions that crop up on the thread will be set on the future.
    try:
        # When timeout is unspecified, the result method waits indefinitely.
        future.result(timeout=30)
    except Exception as e:
        print(
            'Listening for messages on {} threw an Exception: {}.'.format(
                args.subscription, e))