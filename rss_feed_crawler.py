# Import Libraries
import feedparser
import time
from datetime import datetime
import json
from apscheduler.schedulers.blocking import BlockingScheduler
import codecs
import urllib2
from dateutil import parser
import hashlib
import sys
import traceback

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
    print 'encoding:', result.encoding
    print 'number of entries:', len(result.entries)
    print '---'
    for entry in result.entries:
        published_timestamp = None
        try:
            published_timestamp = datetime.fromtimestamp(
                time.mktime(entry.published_parsed))
        except Exception as e:
            pass
        if latest_feed_timestamp is None or published_timestamp is None or \
                        published_timestamp > latest_feed_timestamp:
            filtered_feed.append(entry)

    return filtered_feed


def crawl_and_dump_feed(rss_url, latest_feed_timestamp, kafka_producer=None):
    # Get filtered feed
    filtered_feed = get_and_filter_feed(rss_url, latest_feed_timestamp)

    max_timestamp = latest_feed_timestamp

    json_encoder = json.JSONEncoder()

    # For every feed entry, create/open file and dump data
    i = 0
    for entry in filtered_feed:
        try:
            cdr_data = dict()
            i += 1

            if 'feedburner_origlink' in entry:
                cdr_data['url'] = entry.feedburner_origlink
            else:
                cdr_data['url'] = entry.link
            title = unicode(entry.title).encode('utf-8')

            # Crawl the data from the URL
            try:
                response = urllib2.urlopen(cdr_data['url'])
            except urllib2.HTTPError as e:
                print "[ERROR] #{} {} {} {}".format(i, e.code, title, cdr_data['url'])
                continue

            cdr_data['raw_content'] = response.read()
            cdr_data['team'] = 'usc-isi-i2'
            cdr_data['crawler'] = 'dig-rss-feed-crawler'
            cdr_data['content_type'] = 'text/html'
            published_timestamp = None
            try:
                published_timestamp = datetime.fromtimestamp(
                    time.mktime(entry.published_parsed)).replace(microsecond=000001)
            except:
                published_timestamp = datetime.now().isoformat()
            cdr_data['timestamp'] = datetime.now().isoformat()
            cdr_data['doc_id'] = hashlib.sha256(cdr_data['url']).hexdigest().upper()
            cdr_data['project_name'] = PROJECT_NAME

            # for dig usage
            cdr_data['rss'] = entry
            for k, v in entry.items():
                try:
                    json_encoder.encode(v)
                except:
                    del cdr_data['rss'][k]

            # print cdr_data
            # print json.dumps(cdr_data).encode('utf-8')

            # Add CDR object to kafka queue
            # outfile = codecs.open('output.jl', 'a', 'utf-8')
            # outfile.write(json.dumps(cdr_data).encode('utf-8') + '\n')
            # outfile.close()
            kafka_producer = add_to_kafka_queue(cdr_data, kafka_producer)

            if max_timestamp is None or max_timestamp < published_timestamp:
                max_timestamp = published_timestamp

            print '#{}: {}'.format(i, title)

            # Wait for some time before next call
            time.sleep(WAIT_TIME)

        except Exception as e:
            print "[ERROR] #{} {}".format(i, title)

            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            lines = ''.join(lines)
            print lines

    return max_timestamp


def update_feed_state(latest_feed_state):
    with open(LATEST_FEED_STATE_FILE, 'w') as f:
        f.write(json.dumps(latest_feed_state, indent=4))


def start_crawler():

    print 'Starting crawler'
    kafka_producer = init_kafka()

    # Load the state from the file
    old_feed_state = dict()
    try:
        with open(LATEST_FEED_STATE_FILE, 'r') as f:
            old_feed_state = json.loads(f.read())
    except:
        pass

    # Load all feeds to new latest_feed_state
    latest_feed_state = dict()
    with open(FEED_LIST, 'r') as f:
        for line in f:
            line = line.strip()
            if len(line) > 0:
                latest_feed_state[line] = old_feed_state.get(line, 0)
    update_feed_state(latest_feed_state)

    # For every key in latest_feed_state, crawl the data
    for rss_url in list(latest_feed_state):
        print '\n===\n', rss_url
        latest_state = latest_feed_state[rss_url]
        if latest_state == 0:
            latest_state = None
        else:
            latest_state = parser.parse(latest_state)

        timestamp = crawl_and_dump_feed(rss_url, latest_state, kafka_producer)
        # print 'timestamp',timestamp
        latest_feed_state[rss_url] = str(timestamp) if timestamp else 0

        # Update the state in the file (after every feed)
        update_feed_state(latest_feed_state)


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
    except:
        print 'Error in schedule_crawler()'
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        lines = ''.join(lines)
        print lines


if __name__ == '__main__':
    schedule_crawler()
