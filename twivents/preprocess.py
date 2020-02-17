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
        text = self.metadata(tweet)

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

    def stemmer(self, text):
        return [self.stem.stem(w) for w in text]

    @staticmethod
    def metadata(tweet):
        slices = []
        entities = tweet['entities']

        metadata = ['urls', 'hashtags', 'user_mentions', 'symbols']
        # get indices of metadata
        for metatype in metadata:
            if metatype in entities:
                for m in entities[metatype]:
                    slices += [{'start': m['indices'][0], 'stop': m['indices'][1]}]

        # special case for media
        if 'extended_entities' in tweet:
            entities = tweet['extended_entities']
        if 'media' in entities:
            for m in entities['media']:
                slices += [{'start': m['indices'][0], 'stop': m['indices'][1]}]

        # remove content from text using indices
        # sort so don't have to use offsets
        slices = sorted(slices, key=lambda x: -x['start'])
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
    'l': Preprocessor.lemmatizer,
    'm': Preprocessor.stemmer
}
