import numpy as np
from datetime import datetime
import pytz
import time

def format_timestamp(created_at):
    return datetime.strptime(created_at,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)


class Document():
    def __init__(self, doc):
        self.id = doc['id']
        self.terms = doc['processed_text']
        self.timestamp = format_timestamp(doc['created_at'])

class Cluster():
    def __init__(self, doc):
        self.ids = set(doc.id)
        self.num_docs = 1
        # self.terms = dict()

        self.newest_timestamp = doc.timestamp
        self.oldest_timestamp = doc.timestamp
        self.mean_timestamp = doc.timestamp

        self.centroid = doc.vec

    def add_doc(self, doc):
        self.ids.add(doc.id)
        self.num_docs += 1

        self.newest_timestamp = max(self.newest_timestamp, doc.timestamp)
        self.oldest_timestamp = min(self.oldest_timestamp, doc.timestamp)
        self.mean_timestamp += (doc.timestamp - self.mean_timestamp) / self.num_docs

        self.centroid += (doc.vec - self.centroid) / self.num_docs
        
        # self.add_terms(doc.terms)

    # def add_terms(self, terms):
    #     for term, freq in terms:
    #         if term in self.terms.keys():
    #             self.terms[term] += freq
    #         else:
    #             self.terms[term] = freq
        

    def summarize(self):
        pass

# change name
class OnlineClusterer():
    def __init__(self, threshold=0):
        self.threshold = threshold
        self.clusters = []
        # self.model = # load trained vector

    def add_doc(self, doc):
        doc = Document(doc)

        # calculate similarity for each cluster
        sims = [self.similarity(doc.vec, c) for c in self.clusters]
        
        # add tweet to cluster with max similarity if over threshold
        max_sim = max(sims)
        if max_sim > self.threshold:
            max_index = sims.index(max_sim)
            self.clusters[max_index].add_doc(doc)
        else: # create new cluster
            self.add_cluster(doc)

    def add_cluster(self, doc):
        self.clusters.append(Cluster(doc))

    @staticmethod
    def similarity(vec, cluster):
        pass



def new_doc(i):
    return Document({
        'id': i,
        'created_at': datetime.now()
    })

if __name__ == "__main__":
    i = 1
    doc = new_doc(i)
    cluster = Cluster(doc)
    while True:
        i += 1
        cluster.add_doc(new_doc(i))
        time.sleep(1)
