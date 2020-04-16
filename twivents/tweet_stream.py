
from tweepy import Stream
from tweepy import AppAuthHandler, OAuthHandler
from tweepy import API
from tweepy.streaming import StreamListener

import streamz
import time, datetime, re

from .preprocess import Preprocessor
from .credentials import consumer_key, consumer_secret, access_token, access_secret

class TwitterStream():
    def __init__(self, output):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        api = API(auth)
        api.verify_credentials()

        listener = Listener(output)
        self.stream = Stream(auth=api.auth, listener=listener, tweet_mode="extended")

    def start(self):
        try:
            print('Start streaming...')
            self.stream.sample(languages=['en'])
        except KeyboardInterrupt:
            print()
            print("Stopped.")
        finally:
            print('Done.')
            self.stream.disconnect()


class Listener(StreamListener):
    def __init__(self, output):
        super().__init__()
        self.output = output

    def on_status(self, status):
        status = status._json
        if 'retweeted_status' not in status:
            if 'extended_tweet' in status:
                status['text'] = status['extended_tweet']['full_text']
                status['entities'] = status['extended_tweet']['entities']
            self.output.emit(status)

    def on_error(self, status_code):
        print(status_code)
        return False
