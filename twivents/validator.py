from collections import Counter
import numpy as np

from .preprocessor import Preprocessor
from .embedding_model import EmbeddingModel
from .online_cluster import Document

class Validator():
    def __init__(self):
        self.pre = Preprocessor()
        self.model = EmbeddingModel()

        self.filtered = Counter()

    def check(self, status, label=None):
        # if tweet is a retweet, adds nothing, only care about original tweet
        if 'retweeted_status' in status:
            self.filtered.update(['retweet'])
            return

        # only care about english tweets
        if status['lang'] != "en":
            self.filtered.update(['language'])
            return

        status = self.pre.process_tweet(status)

        # discard tweets with little content
        if len(status['p_text']) < 3:
            self.filtered.update(['length'])
            return

        vec = self.model.average(status['p_text'])

        # no words in vector model, tweet can't be represented
        if np.isnan(vec).any():
            self.filtered.update(['model_vocab'])
            return

        self.filtered.update(['valid'])
        doc = Document(status, vec, label)
        return doc

    def reset(self):
        self.filtered = Counter()


        
