import json
import os
import streamz
from dask.distributed import Client
import time

from .utils.config import parse_args, parse_opt
from .utils import tweets
from .preprocess import Preprocessor

from .utils import timing
from .tweet_stream import TwitterStream


def preprocess(inpath, actions, outpath=''):
    pre = Preprocessor(actions)

    proc, lang, dup, non = 0, 0, 0, 0
    tweet_ids = set()
    # open up file
    
    outpath = inpath + '_preprocessed' # temp, doesn't take extensions into account

    with open(inpath, 'r', encoding="utf-8") as infile:
        with open(outpath, 'w', encoding="utf-8") as outfile:
            for cnt, line in enumerate(infile):
                tweet = tweets.load(line)
                # only care about english tweets
                if tweet['lang'] != "en":
                    lang += 1
                    continue

                tweet_id = tweet['id_str']
                if tweet_id in tweet_ids: # will cause memory issues in future
                    dup += 1
                    continue
                tweet_ids.add(tweet_id)

                p_text = pre.process_tweet(tweet)
                if not p_text:
                    non += 1
                    continue

                tweet = tweets.process(tweet)
                tweet['processed_text'] = p_text

                json.dump(tweet, outfile, ensure_ascii=False)
                outfile.write('\n')
                proc += 1

    print("preprocessed {}/{} tweets, removed: {} non-english, {} duplicates, {} non-content".format(proc, cnt+1, lang, dup, non))

    # apply actions to file

def cluster(inpath):
    pass


def all(inpath, actions, outpath=''):
    preprocess(inpath, actions, outpath)

def live(pre_actions):
    preprocessor = Preprocessor(pre_actions)
    client = Client(processes=False)
    source = streamz.Stream()
    source.scatter().map(preprocessor.process_tweet).buffer(5).gather().sink(print)

    twitter_stream = TwitterStream(source)
    twitter_stream.start()

def main():
    options = parse_args()

    print(options)
    modes = ['all', 'preprocess', 'live']
    mode = parse_opt("mode", modes, options.mode)

    # inpath = os.path.join(os.getcwd(), options.input)

    # dynamically call mode
    # preprocess(inpath, options.preprocess)  # temp
    live(options.preprocess)


if __name__ == '__main__':
    main()
