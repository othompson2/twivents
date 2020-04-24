import numpy as np
from datetime import datetime, timedelta
import pytz
from collections import Counter
from sortedcontainers import SortedList
import pprint
import ray

from .utils.utils import format_timestamp, cosine_similarity
from .preprocessor import Preprocessor
from .embedding_model import EmbeddingModel

class Document():
    def __init__(self, doc, vec, label=None):
        self.id = doc['id']
        self.terms = set(doc['p_text'])
        self.timestamp = format_timestamp(doc['created_at'])
        self.vec = vec
        self.label = label if label else self.id


class Cluster():
    def __init__(self, doc):
        # need unique id for cluster
        self.ids = {doc.id}
        self.labels = Counter([doc.label])
        self.doc_count = 1
        self.mature = False
        self.seen = False
        
        self.terms = Counter(doc.terms) # counter for terms in cluster
        self.top_terms = self.terms.most_common(20)

        self.newest_timestamp = doc.timestamp
        self.oldest_timestamp = doc.timestamp
        self.mean_timestamp = doc.timestamp
        self.state = 1

        self.centroid = np.copy(doc.vec) # value not reference

    def add_doc(self, doc):
        self.ids.add(doc.id)
        self.labels.update([doc.label])
        self.doc_count += 1

        # TODO: HARDCODED
        if self.doc_count >= 25:
            self.mature = True
        
        self.terms += Counter(doc.terms)
        self.top_terms = self.terms.most_common(20)
        
        # update timestamps
        self.newest_timestamp = max(self.newest_timestamp, doc.timestamp)
        self.oldest_timestamp = min(self.oldest_timestamp, doc.timestamp)
        self.mean_timestamp += (doc.timestamp - self.mean_timestamp) / self.doc_count
        self.update_state()
        
        # recalculate doc.vec and weight based on word count/weight in cluster
        # will cause more common/important words to influence the vector more
        self.centroid += (doc.vec - self.centroid) / self.doc_count
        
    def merge(self, cluster):
        self.ids.update(cluster.ids)
        self.labels += cluster.labels
        
        self.terms += cluster.terms
        self.top_terms = self.terms.most_common(20)
        
        self.newest_timestamp = max(self.newest_timestamp, cluster.newest_timestamp)
        self.oldest_timestamp = min(self.oldest_timestamp, cluster.oldest_timestamp)
        mean_diff = cluster.mean_timestamp - self.mean_timestamp
        self.mean_timestamp += (self.doc_count / (self.doc_count + cluster.doc_count)) * mean_diff
        self.update_state()
        
        centroid_diff = cluster.centroid - self.centroid
        self.centroid += (self.doc_count / (self.doc_count + cluster.doc_count)) * centroid_diff
        
        # update count after finished using seperate counts
        self.doc_count += cluster.doc_count
        
    def update_state(self):
        if self.mean_timestamp != self.newest_timestamp:
            oldest_diff = self.mean_timestamp - self.oldest_timestamp
            newest_diff = self.newest_timestamp - self.mean_timestamp
            self.state = (oldest_diff / newest_diff)
        else:
            self.state = 1
        
    def summarize(self):
        s = dict()
        s['doc_count'] = self.doc_count
        s['terms'] = self.top_terms
        s['oldest_time'] = self.oldest_timestamp
        s['newest_time'] = self.newest_timestamp
        s['mean_time'] = self.mean_timestamp
        s['labels'] = self.labels
        s['centroid'] = self.centroid
        return s
    
    def alive(self):
        # time based decay, remove when not alive
        # maybe return probability instead so can set threshold
        return True


@ray.remote
def max_cluster(vec, cluster_vecs):
    # calculate similarity to each cluster
    sims = np.asarray([cosine_similarity(vec, cluster_vec) for cluster_vec in cluster_vecs])
    max_index = sims.argmax()
    max_sim = sims[max_index]

    return max_sim, max_index

class OnlineClusterer():
    def __init__(self, threshold=0.6):
        self.threshold = threshold
        self.clusters = []

        self.current_time = datetime.now()

    def add_docs(self, docs):
        self.current_time = docs[-1].timestamp

        new_clusters = []

        if self.clusters:
            cluster_vecs = ray.put([cluster.centroid for cluster in self.clusters])
            cluster_sims = ray.get([max_cluster.remote(doc.vec, cluster_vecs) for doc in docs])

            for doc, (max_sim, max_index) in zip(docs, cluster_sims):
                # TODO: HARDCODED
                if max_sim > self.threshold:
                    self.clusters[max_index].add_doc(doc)
                else:
                    if new_clusters:
                        sims = np.asarray([cosine_similarity(doc.vec, cluster.centroid) for cluster in new_clusters])
                        max_index = sims.argmax()
                        max_sim = sims[max_index]

                        if max_sim > self.threshold:
                            new_clusters[max_index].add_doc(doc)
                            continue

                    new_clusters.append(self.add_cluster(doc))

        else:
            for doc in docs:
                if new_clusters:
                    sims = np.asarray([cosine_similarity(doc.vec, cluster.centroid) for cluster in new_clusters])
                    max_index = sims.argmax()
                    max_sim = sims[max_index]

                    if max_sim > self.threshold:
                        new_clusters[max_index].add_doc(doc)
                        continue

                new_clusters.append(self.add_cluster(doc))
    

    def add_doc(self, doc):
        self.current_time = doc.timestamp
        
        if self.clusters:
            # calculate similarity to each cluster
            sims = np.asarray([cosine_similarity(doc.vec, cluster.centroid) for cluster in self.clusters])
            max_index = sims.argmax()
            max_sim = sims[max_index]

            # add tweet to cluster with max similarity if over threshold, return
            if max_sim > self.threshold:
                self.clusters[max_index].add_doc(doc)
                return
                
        # else create new cluster
        self.add_cluster(doc)

    def add_cluster(self, doc):
        cluster = Cluster(doc)
        self.clusters.append(cluster)
        return cluster

    def remove_clusters(self):
        for i in range(len(self.clusters)-1, 0, -1):
            cluster = self.clusters[i]

            # TODO: HARDCODED
            if not cluster.mature and cluster.seen:
                self.clusters.pop(i)
                continue

            cluster.seen = True

            # cluster.newest_timestamp < self.current_time - timedelta(minutes=2)
        
    def merge_clusters(self):
        # cycle through clusters in reverse order so indices remain consistent when a cluster 
        # is removed in the loop. merge from left into right so multiple clusters can be merged
        # in one pass
        for i in range(len(self.clusters)-1, 0, -1):
            c1 = self.clusters[i]

            if c1.mature:
                sims = np.asarray([cosine_similarity(c1.centroid, c2.centroid) if c2.mature else 0 for c2 in self.clusters[:i]])
                max_index = sims.argmax()
                max_sim = sims[max_index]
                
                # TODO: HARDCODED
                if max_sim > 0.85:
                    # pprint.pprint([max_sim, self.clusters[i].top_terms, self.clusters[max_index].top_terms])
                    self.clusters[max_index].merge(c1)
                    self.clusters.pop(i)
        
    def summarize(self):
        s = dict()
        s['cluster_count'] = len(self.clusters)
        s['mature_cluster_count'] = len([cluster for cluster in self.clusters if cluster.mature])
        s['clusters'] = [cluster.summarize() for cluster in self.clusters if cluster.mature]
        return s
    
    @staticmethod
    def similarity(doc, cluster):
        return cosine_similarity(doc.vec, cluster.centroid)

    @staticmethod
    def keyword_similarity(doc, cluster):
        # each tweet keywords are just the words
        
        # only compare top 10 (maybe 20/30) words
        # actually store 1000+ words to give them a chance to 'bubble' to the top
        # when reach 1000 words, disregard oldest word with only 1 occurance and replace it with the new one
        # cluster is mature when have 1000+ words with more than 1 occurance so dont care anymore
        
        # rather than keeping count, could set a base value then:
        # decrease the value when a tweet is compared which countains the word
        # increase the value x2 when a tweet is added which contains the word
        # means words which influence whether a tweet is added improve, fixing the meaning of the cluster
        # words which are more common in multiple clusters become less important
        
        pass
    
    @staticmethod
    def time_likelihood(doc, cluster):
        # need to take doc_count into account
        
        # rising/falling method
        # divide duration of first half by duration of second half (-1), rising = >0, falling = <0
        # have to cap rising
    
        # alternatively only look at second half width
        # would have to scale to a reasonable number as dont know what would be 'active' etc
        
        pass


@ray.remote
def best_cluster(vec, cluster_vecs):
    # calculate similarity to each cluster
    sims = np.asarray([cosine_similarity(vec, cv) for cv in cluster_vecs])
    max_index = sims.argmax()
    max_sim = sims[max_index]

    return max_index, max_sim