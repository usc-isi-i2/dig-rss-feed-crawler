# rss feed crawler
FROM python:2.7-slim

RUN apt-get update && apt-get install -y \
    git \
    wget \
    curl \
    vim

WORKDIR /app
ADD . /app
RUN pip install -r /app/requirements.txt

#EXPOSE 8080

CMD ["python", "rss_feed_crawler.py"]
