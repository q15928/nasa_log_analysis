"""
Send the log records to Pub/Sub
Topic: nasa_log

How to run:
python send_nasa_log.py --input ./data/access_log_sample --project $GOOGLE_CLOUD_PROJECT --topic nasa_log
"""

import datetime
import logging
import argparse
from google.cloud import pubsub


def publish(publisher, topic, events):
    num_obs = len(events)
    if num_obs == 0:
        return
    logging.info('Sending {0} events to {1}'.format(num_obs, topic))
    for event_data in events:
        publisher.publish(topic, event_data)

def send_log(filepath, topic, num_obs=20):
    with open(filepath, 'rb') as f:
        to_publish = []
        cnt = 0
        for line in f:
            to_publish.append(line)
            cnt += 1
            if cnt >= num_obs:
                break

    publish(publisher, topic, to_publish)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=('Send data to Cloud Pub/Sub in small groups, '
                     'simulating real-time behavior'))
    parser.add_argument(
        '--project', 
        dest='project',
        help='Example: --project $DEVSHELL_PROJECT_ID', 
        required=True)
    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to send to PubSub',
        required=True)
    parser.add_argument(
        '--topic',
        dest='topic',
        help=('PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))


    args = parser.parse_args()

    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(args.project, args.topic)
    try:
        publisher.get_topic(event_type)
        logging.info('Reusing pub/sub topic {}'.format(args.topic))
    except:
        publisher.create_topic(event_type)
        logging.info('Creating pub/sub topic {}'.format(args.topic))

    # send log
    send_log(args.input, event_type, num_obs=200)