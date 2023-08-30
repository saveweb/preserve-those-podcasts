# preserve-those-podcasts

Podcast archiving tool!

## Why "preserve-those-podcasts"

Inspired by <http://preservethispodcast.org>

## Requirements

* Py>=3.8
* requests
* feedparser
* rich
* internetarchive
* pyrfc6266

* ffmpeg (`ffprobe`)

## Installation

```bash
pip install PreserveThosePod
```

## Usage

### Quickstart

To archive a podcast, run:

```bash
podcastsPreserve --add <rss_feed_url> # download all episodes
podcastsPreserve --update # download new episodes
podcastsUpload # upload to archive.org
```
