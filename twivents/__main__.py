import pprint
import numpy as np
import ray

from .utils import config
from .utils.utils import cosine_similarity
from .utils.timing import *

from .dataset_methods import *
from .validator import Validator

def main():
    args = config.parse()

    # use ray to enable multiprocessing of tweets
    if args.multiprocessing:
        ray.init(num_cpus=args.cores)

    # TODO: get filenames dynamically
    filenames = [
        'background',
        'dallas',
        'dorian',
        'fifa',
        'notre_dame'
    ]

    # filenames = ['merged_background_0.4']

    validator = Validator(args)

    if args.mode == "dataset":
        altered = False
        if args.filter:
            altered = filter_datasets(validator, filenames)
            filenames = [filename + "_filtered" for filename in filenames]

        if args.merge:
            merge_datasets(filenames, altered, args)
            stream = DatasetStream(validator, "merged", args)
        elif len(filenames) == 1:
            stream = DatasetStream(validator, filenames[0], args)
        else:
            stream = MultiDatasetStream(validator, filenames, args)

        if args.multiprocessing:
            stream.run_multi()
        else:
            stream.run()

    elif args.mode == "live":
        pass
    else:
        print("Invalid mode")

        



    

if __name__ == '__main__':
    main()
