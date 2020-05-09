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
    def __init__(self, status, vec):
        self.id = status['id']
        self.terms = set(status['p_text'])
        self.timestamp = format_timestamp(status['created_at'])
        self.vec = vec
        self.label = status['label'] if 'label' in status else 'unlabelled'


class Cluster():
    def __init__(self, mature_threshold, doc):
        # need unique id for cluster
        self.ids = {doc.id}
        self.labels = Counter([doc.label])
        self.doc_count = 1
        self.mature_threshold = mature_threshold
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

        if self.doc_count >= self.mature_threshold:
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
        s['labels'] = self.labels
        # s['oldest_time'] = self.oldest_timestamp
        # s['newest_time'] = self.newest_timestamp
        # s['mean_time'] = self.mean_timestamp
        # s['centroid'] = self.centroid
        return s


@ray.remote
def max_cluster_remote(vec, cluster_vecs, threshold):
    # calculate similarity to each cluster
    sims = np.asarray([cosine_similarity(vec, cluster_vec) for cluster_vec in cluster_vecs])
    max_index = sims.argmax()
    max_sim = sims[max_index]

    if max_sim > threshold:
        return max_index

def max_cluster(doc, clusters, threshold):
    # calculate similarity to each cluster
    sims = np.asarray([cosine_similarity(doc.vec, cluster.centroid) for cluster in clusters])
    max_index = sims.argmax()
    max_sim = sims[max_index]

    if max_sim > threshold:
        return max_index

class OnlineClusterer():
    def __init__(self, args):
        self.add_threshold = args.add_threshold
        self.merge_threshold = args.merge_threshold
        self.mature_threshold = args.mature_threshold
        self.merge_frequency = args.merge_frequency
        self.remove_frequency = args.remove_frequency
        self.summary_frequency = args.summary_frequency
        
        self.clusters = []
        self.new_clusters = []

        self.count = 0
        self.current_time = datetime.now()

    def add_doc(self, doc):
        self.current_time = doc.timestamp
        
        if self.clusters:
            max_index = max_cluster(doc, self.clusters, self.add_threshold)
            if max_index:
                self.clusters[max_index].add_doc(doc)
                return
                
        # else create new cluster
        cluster = Cluster(self.mature_threshold, doc)
        self.clusters.append(cluster)

    def add_docs(self, docs):
        self.count += len(docs)
        self.current_time = docs[-1].timestamp
        self.new_clusters = []

        if self.clusters:
            cluster_vecs = ray.put([cluster.centroid for cluster in self.clusters])
            cluster_sims = ray.get([max_cluster_remote.remote(doc.vec, cluster_vecs, self.add_threshold) for doc in docs])

            for doc, max_index in zip(docs, cluster_sims):
                if max_index:
                    self.clusters[max_index].add_doc(doc)
                else:
                    self.form_clusters([doc])

        else:
            self.form_clusters(docs)

        self.check_triggers()

    def form_clusters(self, docs):
        for doc in docs:
            if self.new_clusters:
                max_index = max_cluster(doc, self.new_clusters, self.add_threshold)
                if max_index:
                    self.new_clusters[max_index].add_doc(doc)
                    continue

            # else create new cluster
            cluster = Cluster(self.mature_threshold, doc)
            self.new_clusters.append(cluster)
            self.clusters.append(cluster)

    def check_triggers(self):
        # remove trigger
        if self.count % self.remove_frequency == 0:
            self.remove_clusters()

        # merge trigger
        if self.count % self.merge_frequency == 0:
            self.merge_clusters()

        # summary trigger
        if self.count % self.summary_frequency == 0:
            self.summarize()

    def remove_clusters(self):
        for i in range(len(self.clusters)-1, 0, -1):
            cluster = self.clusters[i]

            if not cluster.mature and cluster.seen:
                self.clusters.pop(i)
                continue

            cluster.seen = True
        
    def merge_clusters(self):
        # cycle through clusters in reverse order so indices remain consistent when a cluster 
        # is removed in the loop. merge from left into right so multiple clusters can be merged
        # in one pass
        for i in range(len(self.clusters)-1, 0, -1):
            c1 = self.clusters[i]

            if c1.mature:
                # mature_clusters = [cluster if cluster.mature for cluster in self.clusters[:i]]
                sims = np.asarray([cosine_similarity(c1.centroid, c2.centroid) if c2.mature else 0 for c2 in self.clusters[:i]])
                max_index = sims.argmax()
                max_sim = sims[max_index]
                
                if max_sim > self.merge_threshold:
                    self.clusters[max_index].merge(c1)
                    self.clusters.pop(i)
        
    def summarize(self):
        summary = dict()

        summary['cluster_count'] = len(self.clusters)

        mature_clusters = [cluster for cluster in self.clusters if cluster.mature]
        summary['mature_cluster_count'] = len(mature_clusters)

        clusters = [cluster.summarize() for cluster in self.clusters if cluster.mature]
        clusters.sort(key=lambda c: c['doc_count'], reverse=True)
        summary['clusters'] = clusters

        # need to use folder from cmd
        with open(f'output/summary_{self.count}.json', 'w') as out:
            pprint.pprint(summary, stream=out)

    def finialize(self):
        for cluster in self.clusters:
            cluster.seen = True
        self.remove_clusters()
        self.merge_clusters()
        self.summarize()
    
    @staticmethod
    def similarity(doc, cluster):
        return cosine_similarity(doc.vec, cluster.centroid)
    
    @staticmethod
    def time_likelihood(doc, cluster):
        # need to take doc_count into account
        
        # rising/falling method
        # divide duration of first half by duration of second half (-1), rising = >0, falling = <0
        # have to cap rising
    
        # alternatively only look at second half width
        # would have to scale to a reasonable number as dont know what would be 'active' etc
        
        pass