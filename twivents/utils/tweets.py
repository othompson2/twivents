import json
import re


def load(status):
    # status = status._json
    status = json.loads(status, encoding="utf-8")
    # print(status)

    # if tweet is a retweet, adds nothing, only care about original tweet
    if 'retweeted_status' in status:
        status = status['retweeted_status']

    return status


def process(status):
    # only care about 'relevant' tweets
    # need to filter out obvious and less obvious tweets

    # user followers count
    tweet = {
        'id': status['id_str'],
        'created_at': status['created_at'],
        'text': status['full_text'],
        'user': status['user']['id_str'],
        'hashtags': [h['text'] for h in status['entities']['hashtags']],
        'mentions': [re.sub(r'[^A-Za-z0-9\s]+', '', m['name']) for m in status['entities']['user_mentions']]
        # 'favourite_count': status['favorite_count'],
        # 'retweet_count': status['retweet_count'],
        # 'possibly_sensitive': hasattr(status, 'possibly_sensitive'),
        # 'place': status['place']['full_name'] if status['place'] is not None else None,
        # 'coordinates': status['coordinates']['coordinates'] if status['coordinates'] is not None else None,
        # 'reply_to': status['in_reply_to_id'] if hasattr(status, 'in_reply_to_id') else None
    }

    return tweet
