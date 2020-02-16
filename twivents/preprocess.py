import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from .utils.config import parse_letters

class Preprocessor():

    def __init__(self, actions):
        parse_letters(pos_actions.keys(), actions)
        self.actions = [v for (k, v) in pos_actions.items() if k in actions] # maintains order

        if 's' in actions:
            self.s_words = set(stopwords.words('english'))

        if 'l' in actions:
            self.lem = WordNetLemmatizer()

    def process_tweet(self, tweet):
        text = self.remove_metadata(tweet)

        return self.process_text(text)

    def process_text(self, text):
        for func in self.actions:
            text = func(self, text)

        return text

    def case(self, text):
        return text.lower()

    def numbers(self, text):
        return re.sub(r'\d+', '', text)

    def punctuation(self, text):
        # return text.translate(str.maketrans('', '', string.punctuation))
        return re.sub(r'[^A-Za-z0-9\s]+', '', text)

    def whitespaces(self, text):
        return text.strip()

    def tokenize(self, text):
        return word_tokenize(text)

    def stop_words(self, text):
        return [w for w in text if w not in self.s_words]

    def lemmatizer(self, text):
        return [self.lem.lemmatize(w) for w in text]

    # stemmer


    @staticmethod
    def remove_metadata(tweet):
        slices = []
        entities = tweet['entities']
        #Strip out the urls.
        if 'urls' in entities:
            for url in entities['urls']:
                slices += [{'start': url['indices'][0], 'stop': url['indices'][1]}]
        
        #Strip out the hashtags.
        if 'hashtags' in entities:
            for tag in entities['hashtags']:
                slices += [{'start': tag['indices'][0], 'stop': tag['indices'][1]}]
        
        #Strip out the user mentions.
        if 'user_mentions' in entities:
            for men in entities['user_mentions']:
                slices += [{'start': men['indices'][0], 'stop': men['indices'][1]}]
        
        #Strip out the symbols.
        if 'symbols' in entities:
            for sym in entities['symbols']:
                slices += [{'start': sym['indices'][0], 'stop': sym['indices'][1]}]
        
        #Strip out the media.
        if 'extended_entities' in tweet:
            entities = tweet['extended_entities']
        if 'media' in entities:
            for med in entities['media']:
                slices += [{'start': med['indices'][0], 'stop': med['indices'][1]}]
        
        # Sort the slices from highest start to lowest.
        slices = sorted(slices, key=lambda x: -x['start'])
        
        #No offsets, since we're sorted from highest to lowest.
        text = tweet['full_text']
        for s in slices:
            text = text[:s['start']] + text[s['stop']:]
            
        return text


pos_actions = {
    'c': Preprocessor.case,
    'n': Preprocessor.numbers,
    'p': Preprocessor.punctuation,
    'w': Preprocessor.whitespaces,
    't': Preprocessor.tokenize,
    's': Preprocessor.stop_words,
    'l': Preprocessor.lemmatizer
}
