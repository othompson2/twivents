from pkg_resources import resource_string
import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer


class Preprocessor():

    def __init__(self):
        words = resource_string(__name__, 'data/stopwords.txt').decode("utf-8")
        self.s_words = words.split()
        # self.s_words = set(stopwords.words('english'))

    def process_tweet(self, tweet):
        p_text = self.remove_metadata(tweet)
        p_text = re.sub(r'[^A-Za-z\s]+', '', p_text) # punctuation and numbers
        p_text = p_text.strip() # whitespaces
        p_text = word_tokenize(p_text)
        p_text = self.stop_words(p_text)

        p_tweet = {
            'id': tweet['id_str'],
            'created_at': tweet['created_at'],
            'text': tweet['full_text'] if 'full_text' in tweet else tweet['text'],
            'p_text': p_text,
            'hashtags': [h['text'] for h in tweet['entities']['hashtags']],
            'label': tweet['label'] if 'label' in tweet else 'unlabelled'
        }

        return p_tweet

    # need to change this idea of a stop list to remove more generic words from the vector
    # however leave words to be used as top terms that can add meaning
    # also need to keep words which dont have a vector as they can add meaning to the top terms
    def stop_words(self, text):
        return [w for w in text if w not in self.s_words]

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

        # get indices of metadata
        for metatype in metadata:
            if metatype in entities:
                for m in entities[metatype]:
                    slices += [{'start': m['indices'][0], 'stop': m['indices'][1]}]

        # # special case for media in extended
        if 'extended_entities' in tweet:
            entities = tweet['extended_entities']
        if 'media' in entities:
            for m in entities['media']:
                slices += [{'start': m['indices'][0], 'stop': m['indices'][1]}]

        text = tweet['full_text'].lower() if 'full_text' in tweet else tweet['text'].lower()

        # remove/replace content from text using indices
        # sort so don't have to use offsets
        slices = sorted(slices, key=lambda x: -x['start'])
        for s in slices:
            text = text[:s['start']] + text[s['stop']:]

        return text