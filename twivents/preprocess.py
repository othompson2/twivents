import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer

from .utils.config import parse_letters


class Preprocessor():

    def __init__(self, actions):
        parse_letters(pos_actions.keys(), actions)
        self.actions = [v for (k, v) in pos_actions.items()
                        if k in actions]  # maintains order

        if 's' in actions:
            self.s_words = set(stopwords.words('english'))

        if 'l' in actions:
            self.lem = WordNetLemmatizer()

        if 'm' in actions:
            self.stem = PorterStemmer()

    def process_tweet(self, tweet):
        print("processing", tweet['id'])

        p_text = self.remove_metadata(tweet)
        p_text = re.sub(r'[^A-Za-z\s]+', '', p_text) # punctuation and numbers
        p_text = p_text.strip() # whitespaces
        p_text = word_tokenize(p_text)

        for func in self.actions:
            p_text = func(self, p_text)

        p_tweet = {
            'id': tweet['id_str'],
            'created_at': tweet['created_at'],
            'text': tweet['text'],
            'p_text': p_text,
            'hashtags': [h['text'] for h in tweet['entities']['hashtags']],
            'mentions': [re.sub(r'[^A-Za-z\s]+', '', m['name']) for m in tweet['entities']['user_mentions']] # dont leave this
        }

        return p_tweet

    def stop_words(self, text):
        return [w for w in text if w not in self.s_words]

    def lemmatizer(self, text):
        return [self.lem.lemmatize(w) for w in text]

    def stemmer(self, text):
        return [self.stem.stem(w) for w in text]

    @staticmethod
    def remove_metadata(tweet):
        slices = []
        entities = tweet['entities']

        metadata = [
            ('urls', 'URL'),
            ('hashtags', ''),
            ('user_mentions', 'USER'),
            ('symbols', ''),
        ]
        # print(tweet)
        # print(entities['urls'])
        # get indices of metadata
        for metatype, replace in metadata:
            if metatype in entities:
                for m in entities[metatype]:
                    slices += [{'start': m['indices'][0], 'stop': m['indices'][1], 'replace': replace}]

        # # special case for media in extended
        if 'extended_entities' in tweet:
            entities = tweet['extended_entities']
        if 'media' in entities:
            for m in entities['media']:
                slices += [{'start': m['indices'][0], 'stop': m['indices'][1], 'replace': ''}]

        text = tweet['text'].lower()

        # remove/replace content from text using indices
        # sort so don't have to use offsets
        slices = sorted(slices, key=lambda x: -x['start'])
        for s in slices:
            text = text[:s['start']] + s['replace'] + text[s['stop']:]

        return text



pos_actions = {
    's': Preprocessor.stop_words,
    'l': Preprocessor.lemmatizer,
    'm': Preprocessor.stemmer
}
