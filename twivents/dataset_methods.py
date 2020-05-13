import os
import jsonlines
import gzip
import random

from .online_cluster import OnlineClusterer


#temp
def open_reader(filename):
    infile = gzip.open(filename + ".jsonl.gz", "r")
    return jsonlines.Reader(infile)

def open_writer(filename):
    outfile = gzip.open(filename + ".jsonl.gz", "w")
    return jsonlines.Writer(outfile)

def yes_or_no(question):
    while True:
        reply = input(question +' (y/n): ').lower().strip()
        if reply[:1] == 'y':
            return True
        if reply[:1] == 'n':
            return False


def create_gold(filenames, limit):
    for filename in filenames:
        # don't refilter files
        if os.path.exists(filename + '_gold.jsonl.gz'):
            print(filename, "already has gold standard")
            continue

        print("creating gold standard for", filename)
        if not yes_or_no("continue?"):
            break

        reader = open_reader(filename + "_filtered")
        writer = open_writer(filename + "_gold")
        accepted = 0
        skip = input("skip how many?")

        for tweet in reader:
            if skip > 0:
                skip -= 1
                continue

            print(accepted)
            print(tweet["full_text"])

            if yes_or_no("add?"):
                writer.write(tweet)
                accepted += 1

            if accepted == limit:
                break

        reader.close()
        writer.close()

def add_background(filenames, background, background_weight):
    weights = [background_weight, 1-background_weight]
    readers = [open_reader(filename) for filename in filenames]
    filenames = [filename.replace('_filtered', '') for filename in filenames]
    writer = open_writer("merged_background_" + str(background_weight))
    while readers:
        index = random.choices(range(len(readers)), weights, k=1)[0]
        try:
            tweet = readers[index].read(skip_empty=True)
            if 'label' not in tweet:
                tweet['label'] = filenames[index]
            writer.write(tweet)
        except EOFError:
            readers[index].close()
            readers.pop(index)
            weights.pop(index)
            filename = filenames.pop(index)
            if filename == background or len(filenames) == 1:
                break

    for reader in readers:
        reader.close()
    writer.close()

def generate_weights(filenames, args):
    file_count = len(filenames)
    if args.background in filenames:
        weights = [args.background_weight]
        weights.extend([(1 - args.background_weight) / file_count] * (file_count - 1))
    else:
        weights = [1/file_count] * file_count
    return weights

def filter_datasets(validator, filenames):
    altered = False
    for filename in filenames:
        # don't refilter files
        if os.path.exists(filename + '_filtered.jsonl.gz'):
            print(filename, "previously filtered")
            continue
        else:
            altered = True

        reader = open_reader(filename)
        writer = open_writer(filename + "_filtered")

        for tweet in reader:
            if validator.check(tweet):
                writer.write(tweet)

        reader.close()
        writer.close()

        print(filename)
        print(validator.summary())
        validator.reset()
    
    return altered
        
def merge_datasets(filenames, altered, args):
    if os.path.exists('merged.jsonl.gz') and not altered:
        print("previously merged")
        return

    weights = generate_weights(filenames, args)
    readers = [open_reader(filename) for filename in filenames]
    filenames = [filename.replace('_filtered', '') for filename in filenames]
    writer = open_writer("merged")
    while readers:
        index = random.choices(range(len(readers)), weights, k=1)[0]
        try:
            tweet = readers[index].read(skip_empty=True)
            if 'label' not in tweet:
                tweet['label'] = filenames[index]
            writer.write(tweet)
        except EOFError:
            readers[index].close()
            readers.pop(index)
            weights.pop(index)
            filename = filenames.pop(index)
            if filename == args.background:
                break

    for reader in readers:
        reader.close()
    writer.close()

class MultiDatasetStream():
    def __init__(self, validator, filenames, args):
        self.cores = args.cores
        self.weights = generate_weights(filenames, args) # need to make it auto generate
        self.readers = [open_reader(filename) for filename in filenames]
        self.filenames = [filename.replace('_filtered', '') for filename in filenames]
        self.background = args.background

        self.validator = validator
        self.online_cluster = OnlineClusterer(args)

    def run(self):
        while self.readers:
            index = random.choices(range(len(self.readers)), self.weights, k=1)
            try:
                tweet = self.readers[index].read(skip_empty=True)
                tweet['label'] = self.filenames[index]
                doc = self.validator.check(tweet)
                self.online_cluster.add_doc(doc)

            except EOFError:
                self.readers[index].close()
                self.readers.pop(index)
                self.weights.pop(index)
                filename = self.filenames.pop(index)
                if filename == self.background:
                    return

        self.online_cluster.finialize()

        for reader in self.readers:
            reader.close()

    def run_multi(self):
        doc_buffer = []
        while self.readers:
            index = random.choices(range(len(self.readers)), self.weights, k=1)
            try:
                tweet = self.readers[index].read(skip_empty=True)
                tweet['label'] = self.filenames[index]
                doc = self.validator.check(tweet)
                doc_buffer.append(doc)
                if len(doc_buffer) == self.cores:
                    self.online_cluster.add_docs(doc_buffer)
                    doc_buffer = []

            except EOFError:
                self.readers[index].close()
                self.readers.pop(index)
                filename = self.filenames.pop(index)
                if filename == self.background:
                    return

        if len(doc_buffer) == 0:
            self.online_cluster.add_docs(doc_buffer)
        self.online_cluster.finialize()

        for reader in self.readers:
            reader.close()
            

class DatasetStream():
    def __init__(self, validator, filename, args):
        self.cores = args.cores
        self.reader = open_reader(filename)
        
        self.validator = validator
        self.online_cluster = OnlineClusterer(args)

    def run(self):
        print("Running stream...")

        for tweet in self.reader:
            doc = self.validator.check(tweet)
            self.online_cluster.add_doc(doc)

        self.reader.close()
        self.online_cluster.finialize()

    def run_multi(self):
        print("Running stream...")

        doc_buffer = []
        for tweet in self.reader:
            doc = self.validator.check(tweet)
            doc_buffer.append(doc)
            if len(doc_buffer) == self.cores:
                self.online_cluster.add_docs(doc_buffer)
                doc_buffer = []

        self.reader.close()
        self.online_cluster.finialize()
            
