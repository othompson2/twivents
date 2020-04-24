folder = '/media/shares/dissertation/tweets/'
datasets = [(folder + 'notre_dame_fire_tweets.jsonl', 'notre', 0),
            (folder + 'dallas_police_shooting_tweets.jsonl', 'dallas', 0),
            (folder + 'hurricane_florence_tweets.jsonl', 'florence', 0),
            (folder + 'hurricane_dorian_tweets.jsonl', 'dorian', 0),
            (folder + 'fifa_corruption_tweets.jsonl', 'fifa', 0),
            (folder + 'texas_senate_debate_tweets.jsonl', 'texas', 0)]

dr = DatasetsReader(datasets)
oc = OnlineClusterer()

for i in range(100000):
    if i % 10000 == 2:
        oc.merge_clusters()
    tweet, label = dr.get_random()
    oc.add_doc(tweet, label)
    
dr.close()

oc.merge_clusters()


##############

import pprint

summary = oc.summarize()
print(summary['cluster_count'])
for c in sorted(summary['clusters'], key=lambda c: c['doc_count'], reverse=True):
    pprint.pprint({k:v for (k,v) in c.items() if k != 'centroid'})
    print()


##############

cluster_sim = []

for c1 in summary['clusters']:
    for c2 in summary['clusters']:
        if c1 != c2:
            cosine = np.dot(c1['centroid'], c2['centroid']) / (np.linalg.norm(c1['centroid']) * np.linalg.norm(c2['centroid']))
            cluster_sim.append((c1['terms'], c2['terms'], cosine))

for x in sorted(cluster_sim, key=lambda x: x[2], reverse=True):
    pprint.pprint(x)