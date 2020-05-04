import gensim
import numpy as np

class EmbeddingModel():
    def __init__(self):
        # TODO: HARDCODED
        self.model = gensim.models.KeyedVectors.load_word2vec_format('./data/GoogleNews-vectors-negative300.bin', binary=True)
    
    def vocab(self):
        return self.model.vocab
    
    def average(self, terms):
        return np.mean([self.model[t] for t in terms if t in self.model.vocab], axis=0)