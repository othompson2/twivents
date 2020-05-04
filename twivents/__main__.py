# import json
# import os
# import streamz
# from dask.distributed import Client
# import time

# from .utils.config import parse_args, parse_opt
# from .utils import tweets
# from .preprocess import Preprocessor

# from .utils import timing
# from .tweet_stream import TwitterStream


# def preprocess(inpath, actions, outpath=''):
#     pre = Preprocessor(actions)

#     proc, lang, dup, non = 0, 0, 0, 0
#     tweet_ids = set()
#     # open up file
    
#     outpath = inpath + '_preprocessed' # temp, doesn't take extensions into account

#     with open(inpath, 'r', encoding="utf-8") as infile:
#         with open(outpath, 'w', encoding="utf-8") as outfile:
#             for cnt, line in enumerate(infile):
#                 tweet = tweets.load(line)
#                 # only care about english tweets
#                 if tweet['lang'] != "en":
#                     lang += 1
#                     continue

#                 tweet_id = tweet['id_str']
#                 if tweet_id in tweet_ids: # will cause memory issues in future
#                     dup += 1
#                     continue
#                 tweet_ids.add(tweet_id)

#                 p_text = pre.process_tweet(tweet)
#                 if not p_text:
#                     non += 1
#                     continue

#                 tweet = tweets.process(tweet)
#                 tweet['processed_text'] = p_text

#                 json.dump(tweet, outfile, ensure_ascii=False)
#                 outfile.write('\n')
#                 proc += 1

#     print("preprocessed {}/{} tweets, removed: {} non-english, {} duplicates, {} non-content".format(proc, cnt+1, lang, dup, non))

#     # apply actions to file

# def cluster(inpath):
#     pass


# def all(inpath, actions, outpath=''):
#     preprocess(inpath, actions, outpath)

# def live(pre_actions):
#     preprocessor = Preprocessor(pre_actions)
#     client = Client(processes=False)
#     source = streamz.Stream()
#     source.scatter().map(preprocessor.process_tweet).buffer(5).gather().sink(print)

#     twitter_stream = TwitterStream(source)
#     twitter_stream.start()

# def main():
#     options = parse_args()

#     print(options)
#     modes = ['all', 'preprocess', 'live']
#     mode = parse_opt("mode", modes, options.mode)

#     # inpath = os.path.join(os.getcwd(), options.input)

#     # dynamically call mode
#     # preprocess(inpath, options.preprocess)  # temp
#     live(options.preprocess)

import pprint
import numpy as np
import ray

from .utils.utils import cosine_similarity
from .utils.timing import *

from .datasets_reader import DatasetsReader
from .online_cluster import OnlineClusterer
from .validator import Validator

def main():
    ray.init()
    folder = '/media/shares/dissertation/tweets/'
    datasets = [(folder + 'notre_dame_fire_tweets.jsonl.gz', 'notre', 0),
                (folder + 'dallas_police_shooting_tweets.jsonl.gz', 'dallas', 0),
                (folder + 'hurricane_florence_tweets.jsonl.gz', 'florence', 0),
                (folder + 'hurricane_dorian_tweets.jsonl.gz', 'dorian', 0),
                (folder + 'fifa_corruption_tweets.jsonl.gz', 'fifa', 0)]#,
                # (folder + 'texas_senate_debate_tweets.jsonl.gz', 'texas', 0)]

    folder = '/media/shares/dissertation/hose/'
    datasets = [(folder + 'twitter.2013-11-30-20.gz', '2013-11-30-20', 0)]

    dr = DatasetsReader(datasets)
    v = Validator()
    oc = OnlineClusterer()

    buffer = []
    valid = 0
    last_merge = 0
    for i in range(1, 250001):
        if i % 10000 == 0:
            print(i)

        data = dr.get_random()
        if not data:
            continue

        doc = v.check(data[0], data[1])
        if not doc:
            continue

        valid += 1
        
        buffer.append(doc)
        if len(buffer) == 12:
            oc.add_docs(buffer)
            buffer = []

        if valid % 10000 == 0 and last_merge != valid:
            print(valid, "valid")
            last_merge = valid
            oc.remove_clusters()
            oc.merge_clusters()
        
    oc.merge_clusters()
    dr.close()

    summary = oc.summarize()
    print(summary['cluster_count'])
    print(summary['mature_cluster_count'])
    print(v.filtered)
    with open('output.json', 'w') as out:
        for c in sorted(summary['clusters'], key=lambda c: c['doc_count'], reverse=True):
            pprint.pprint({k:v for (k,v) in c.items() if k != 'centroid'}, stream=out)

    # cluster_sim = []

    # for c1 in summary['clusters']:
    #     for c2 in summary['clusters']:
    #         if c1 != c2:
    #             cosine = cosine_similarity(c1['centroid'], c2['centroid'])
    #             cluster_sim.append((c1['terms'], c2['terms'], cosine))

    # with open('comparisons.json', 'w') as out:
    #     for x in sorted(cluster_sim, key=lambda x: x[2], reverse=True):
    #         pprint.pprint(x, stream=out)

if __name__ == '__main__':
    main()
