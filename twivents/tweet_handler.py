from tweepy import AppAuthHandler, OAuthHandler
from tweepy import API

class TweetHandler():
    def __init__(self):
        consumer_key = "42vXncwIab2mU0jNlDMSlhJyL"
        consumer_secret = "ltIJu14OgGoW8tBFSXwbSq18VLJxOONGU0SkqJHBmNdQW2GJzm"
        access_token = "1140605179177385989-bCi0W5WP1X84wPUAM7FlZZi4EsA8gT"
        access_secret = "UraGHhpDQHBUxcJejlyPUeGqn1TruBRE0V01emz9pYPHg"

        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        self.api = API(auth)
        
        self.pre = Preprocessor()
    
    def get_status(self, status_id):
        status = self.api.get_status(status_id, tweet_mode="extended")
        status = status._json
        p_status = self.pre.process_tweet(status)
        return p_status
    
    def get_doc(self, status_id, model):
        p_status = self.get_status(status_id)
        vec = model.average(p_status['p_text'])
        doc = Document(p_status, vec)
        return doc