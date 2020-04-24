import ray
import numpy as np
import time
from collections import defaultdict

def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


ray.init(num_cpus=12)

class OnlineClusterer():
    def __init__(self):
        self.clusters = [np.random.rand(300) for _ in range(10)]
        self.threshold = 0.6

    # def add_doc(self, doc, cluster):
    #     cluster.add_doc(doc)

@ray.remote
def best_cluster(vec, clusters):
    # calculate similarity to each cluster
    sims = np.asarray([cosine_similarity(vec, cluster) for cluster in clusters])
    max_index = sims.argmax()
    max_sim = sims[max_index]

    return clusters[max_index], max_sim
    

oc = OnlineClusterer()
# ray.put(oc)
clusters = ray.put(oc.clusters)
docs = [np.random.rand(300) for _ in range(10000)]
best = []

start = time.time()

for doc in docs:
    # best.append(best_cluster(doc, oc.clusters))
    best.append(best_cluster.remote(doc, clusters))
best = ray.get(best)

print(time.time() - start)
# print(best)
# print()

# def cosine_similarity(vec1, vec2):
#     return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


# ray.init(num_cpus=12)

# @ray.remote
# class OnlineClusterer():
#     def __init__(self):
#         self.clusters = [np.random.rand(300) for _ in range(100)]
#         self.threshold = 0.6

#     # def add_doc(self, doc, cluster):
#     #     cluster.add_doc(doc)

#     def add_docs(self, docs):
#         best = [self.best_cluster(doc) for doc in docs]
#         best = ray.get(best)
#         print(best)

#     def best_cluster(self, vec):
#         # calculate similarity to each cluster
#         sims = np.asarray([cosine_similarity(vec, cluster) for cluster in self.clusters])
#         max_index = sims.argmax()
#         max_sim = sims[max_index]

#         return self.clusters[max_index], max_sim
    
# def main():
#     oc = OnlineClusterer.remote()
#     docs = [np.random.rand(300) for _ in range(10000)]

#     start = time.time()

#     buffer = []
#     for doc in docs:
#         buffer.append(doc)
#         if len(buffer) == 10:
#             oc.add_docs.remote(buffer)

#     print(time.time() - start)
#     # print(best)
#     # print()

# if __name__ == '__main__':
#     main()

# import time 
# import random 
# import ray 

# ray.init(num_cpus = 4) 

# @ray.remote 
# def do_some_work(x): 
#     time.sleep(random.uniform(0, 4)) # Replace this with work you need to do. 
#     return x 

# def process_results(results): 
#     sum = 0 
#     for x in results: 
#         time.sleep(1) # Replace this with some processing code. 
#         sum += x 
#     return sum 

# start = time.time() 
# data_list = ray.get([do_some_work.remote(x) for x in range(4)]) 
# sum = process_results(data_list) 
# print("duration =", time.time() - start, "\nresult = ", sum) 