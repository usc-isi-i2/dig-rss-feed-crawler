#!/usr/bin/env bash

opt="${1}"

source ./VERSION

if [ "$opt" == "build" ]; then
    echo "building image..."
    docker build -t uscisii2/rss-feed-crawler:${RSS_FEED_CRAWLER_VERSION} .
elif [ "$opt" == "push" ]; then
    echo "pushing image..."
    docker push uscisii2/rss-feed-crawler:${RSS_FEED_CRAWLER_VERSION}
elif [ "$opt" == "tag" ]; then
    echo "tagging..."
    git tag ${RSS_FEED_CRAWLER_VERSION}
fi
