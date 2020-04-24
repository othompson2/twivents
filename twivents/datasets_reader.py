from datetime import datetime, timedelta, timezone
import gzip
import jsonlines
import random

from .utils.utils import format_timestamp

# need to deal with end of file
class JsonlReader():
    def __init__(self, file, label, start):
        infile = gzip.open(file, "r")
        self.reader = jsonlines.Reader(infile)
        self.label = label
        
        while True:
            self.next_data = self.reader.read(skip_empty=True)
            if 'created_at' in self.next_data:
                break
        self.next_time = start
        
        org_time = format_timestamp(self.next_data['created_at'])
        self.time_diff = start - org_time
        
    def update_next(self):
        while True:
            self.next_data = self.reader.read(skip_empty=True)
            if 'created_at' in self.next_data:
                break
        self.next_time = format_timestamp(self.next_data['created_at']) + self.time_diff
    
    def close(self):
        self.reader.close()

class DatasetsReader():
    def __init__(self, datasets):
        self.datasets = set()
        self.start_time = datetime.now(timezone.utc)
        for file, label, offset in datasets:
            start = self.start_time + timedelta(minutes=offset)
            self.datasets.add(JsonlReader(file, label, start))
            
    def get_random(self):
        dataset = random.choice(tuple(self.datasets))
        next_data = dataset.next_data
        dataset.update_next()
        return next_data, dataset.label
            
    # need to remove datasets from set when they run out of tweets
    def get_next(self):
        dataset = min(self.datasets, key=lambda dataset: dataset.next_time)
        next_data = dataset.next_data
        dataset.update_next()
        return next_data, dataset.label
    
    def close(self):
        for dataset in self.datasets:
            dataset.close()