import os

from configargparse import ArgParser
from .errors import ArgumentError


def parse_args():
    configpath = os.path.join(os.path.dirname(__file__), './config.ini')
    p = ArgParser(default_config_files=[configpath])

    # generic
    p.add('-c', '--config', is_config_file=True, help='config file path')
    p.add('-v', action='store_true', help='verbose')
    p.add('-i', '--input', help='input file name')
    p.add('-o', '--output', help='output file name')
    p.add('-m', '--mode', help='program mode')
    p.add('-p', '--preprocess', help='preprocess actions to be applied')

    # make a setup function to download nltk resources:
    # stopwords, punkt, wordnet

    return p.parse_args()

    #

def parse_opt(opt_name, opts, input_opt):
    pos_opts = [o for o in opts if o.startswith(input_opt)]
    if len(pos_opts) != 1:
        raise ArgumentError(opt_name, opts)
    
    return pos_opts[0]

def parse_letters(pos_letters, letters):
    if not set(letters).issubset(set(pos_letters)):
        raise ArgumentError(letters, pos_letters)
    
    return True
