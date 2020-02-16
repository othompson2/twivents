import json

from .utils.config import parse_args, parse_opt
from .utils import tweets
from .preprocess import Preprocessor

from .utils import timing


def preprocess(filepath, actions):
    pre = Preprocessor(actions)

    proc, lang, dup, non = 0, 0, 0, 0
    tweet_ids = set()
    # open up file
    # need to get url of directory cmd called from
    with open('/home/olly/VSCode/twivents/data/hurrican_dorian', 'r', encoding="utf-8") as infile:
        with open('temp.txt', 'w', encoding="utf-8") as outfile:
            for cnt, line in enumerate(infile):
                tweet = tweets.load(line)
                # only care about english tweets
                if tweet['lang'] != "en":
                    lang += 1
                    continue

                tweet_id = tweet['id_str']
                if tweet_id in tweet_ids:
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


def all(filepath, actions):
    preprocess(filepath, actions)


def main():
    options = parse_args()

    print(options)
    modes = ['all', 'preprocess']
    mode = parse_opt("mode", modes, options.mode)

    # dynamically call mode

    preprocess(options.filepath, options.preprocess)  # temp


if __name__ == '__main__':
    main()
