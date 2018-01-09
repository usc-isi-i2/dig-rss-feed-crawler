# Import Libraries
import feedparser
import time
from datetime import datetime
import json
from apscheduler.schedulers.blocking import BlockingScheduler
import codecs
import urllib2
from dateutil import parser
import uuid

from kafka import KafkaProducer
from config import *


def init_kafka():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        **OUTPUT_ARGS
    )


def add_to_kafka_queue(cdr_data, kafka_producer=None):
    if kafka_producer is None:
        kafka_producer = init_kafka()

    r = kafka_producer.send(PRODUCER_TOPIC, cdr_data)
    r.get(timeout=60)  # wait till sent

    return kafka_producer


def get_and_filter_feed(rss_url, latest_feed_timestamp):
    filtered_feed = list()

    result = feedparser.parse(rss_url)
    print(len(result.entries))
    for entry in result.entries:
        published_timestamp = datetime.fromtimestamp(
            time.mktime(entry.published_parsed))
        if latest_feed_timestamp is None or published_timestamp > latest_feed_timestamp:
            filtered_feed.append(entry)

    return filtered_feed


def crawl_and_dump_feed(rss_url, latest_feed_timestamp, kafka_producer=None):
    # Get filtered feed
    filtered_feed = get_and_filter_feed(rss_url, latest_feed_timestamp)

    max_timestamp = latest_feed_timestamp

    # For every feed entry, create/open file and dump data
    for entry in filtered_feed:
        try:
            cdr_data = dict()
            if 'feedburner_origlink' in entry:
                cdr_data['url'] = entry.feedburner_origlink
            else:
                cdr_data['url'] = entry.link

            cdr_data['doc_id'] = entry.title

            # Crawl the data from the URL
            response = urllib2.urlopen(cdr_data['url'])
            cdr_data['raw_content'] = response.read()
            cdr_data['team'] = 'usc-isi-i2'
            cdr_data['crawler'] = 'dig-rss-crawler'
            cdr_data['content_type'] = 'text/html'
            published_timestamp = datetime.fromtimestamp(
                time.mktime(entry.published_parsed)).replace(microsecond=000001)
            cdr_data['timestamp'] = published_timestamp.isoformat()
            cdr_data['doc_id'] = str(uuid.uuid1())
            cdr_data['project_name'] = PROJECT_NAME

            # Add CDR object to kafka queue
            # outfile = codecs.open('output.jl', 'a', 'utf-8')
            # outfile.write(json.dumps(cdr_data).encode('utf-8') + '\n')
            # outfile.close()
            kafka_producer = add_to_kafka_queue(cdr_data, kafka_producer)

            if max_timestamp is None or max_timestamp < published_timestamp:
                max_timestamp = published_timestamp

            print entry.title.encode('utf-8')

            # Wait for some time before next call
            time.sleep(WAIT_TIME)

        except Exception as e:
            print "ERROR - ", entry.title
            print e

    return max_timestamp


def start_crawler():

    print 'Starting crawler'
    kafka_producer = init_kafka()

    # Load the state from the file
    infile = open(LATEST_FEED_STATE_FILE, 'r')
    latest_feed_state = json.load(infile)
    infile.close()

    # For every key in latest_feed_state, crawl the data
    for rss_url in list(latest_feed_state):
        print '---\n', rss_url, '\n---'
        latest_state = latest_feed_state[rss_url]
        if latest_state == 0:
            latest_state = None
        else:
            latest_state = parser.parse(latest_state)

        latest_feed_state[rss_url] = str(
            crawl_and_dump_feed(rss_url, latest_state, kafka_producer))

    # Update the state in the file
    outfile = open(LATEST_FEED_STATE_FILE, 'w')
    outfile.write(json.dumps(latest_feed_state, indent=4))
    outfile.close()


def schedule_crawler(schedule_interval=SCHEDULE_INTERVAL):
    # Run job now
    start_crawler()

    # Schedule a job every 4 hours
    scheduler = BlockingScheduler()
    scheduler.add_job(start_crawler, 'interval', hours=schedule_interval)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print '\nStopping...\n'


if __name__ == '__main__':
    schedule_crawler()
