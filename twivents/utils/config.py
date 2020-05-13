from pkg_resources import resource_filename
from configargparse import ArgParser

def string2bool(value):
    if value.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif value.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError(f'{value} is not a valid boolean value')

def parse():
    configpath = resource_filename(__name__, '../data/config.ini')
    p = ArgParser(default_config_files=[configpath])

    # GENERAL
    general = p.add_argument_group('General')
    general.add('-c', '--config', is_config_file=True, help='config file path')
    general.add('-v', action='store_true', help='verbose')
    general.add('-m', '--mode', help='program mode')
    general.add('--multiprocessing', type=string2bool, nargs='?', const=True, default=False)
    general.add('--cores', type=int)

    # CLUSTERS
    clusters = p.add_argument_group('Clusters')
    clusters.add('--add_threshold', type=float)
    clusters.add('--merge_threshold', type=float)
    clusters.add('--mature_threshold', type=int)
    clusters.add('--merge_frequency', type=int)
    clusters.add('--remove_frequency', type=int)
    clusters.add('--summary_frequency', type=int)

    # EMBEDDING MODEL
    model = p.add_argument_group('Embedding Model')
    model.add('--type', type=str)
    model.add('--source', type=str)

    # DATASETS
    datasets = p.add_argument_group('Datasets')
    datasets.add('--background', type=str)
    datasets.add('--background_weight', type=float)
    datasets.add('--filter', type=string2bool, nargs='?', const=True, default=False)
    datasets.add('--merge', type=string2bool, nargs='?', const=True, default=False)

    # p.add('-i', '--input', help='input file name')
    # p.add('-o', '--output', help='output file name')

    # make a setup function to download nltk resources:
    # stopwords, punkt, wordnet

    return p.parse_args()