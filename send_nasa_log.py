import datetime
import logging
import argparse
from google.cloud import pubsub

TOPIC = 'nasa_log'
INPUT = './data/access_log_sample'

def publish(publisher, topic, events):
    num_obs = len(events)
    if num_obs == 0:
        return
    logging.info('Sending {0} events to {1}'.format(num_obs, topic))
    for event_data in events:
        publisher.publish(topic, event_data)

def send_log(topic, num_obs=20):
    with open(INPUT, 'rb') as f:
        to_publish = []
        cnt = 0
        for line in f:
            to_publish.append(line)
            cnt += 1
            if cnt >= num_obs:
                break

    publish(publisher, topic, to_publish)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send data to Cloud Pub/Sub in small groups, simulating real-time behavior')
    parser.add_argument('--project', help='Example: --project $DEVSHELL_PROJECT_ID', required=True)
    args = parser.parse_args()

    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(args.project, TOPIC)
    try:
        publisher.get_topic(event_type)
        logging.info('Reusing pub/sub topic {}'.format(TOPIC))
    except:
        publisher.create_topic(event_type)
        logging.info('Creating pub/sub topic {}'.format(TOPIC))

    # send log
    send_log(event_type)