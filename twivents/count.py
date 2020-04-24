
import gzip, jsonlines
import sys

# if len(sys.argv) != 2:
#     print('WARNING: must specify one (gzipped) file as argument!')
#     print('- exiting!')
#     sys.exit()

# f_path = sys.argv[1]

f_path = '/media/shares/dissertation/hose/twitter.2013-11-30-20.gz'

linecount = 0
with gzip.open(f_path, "r") as infile:
    readlines = jsonlines.Reader(infile)
    for tweet in readlines:
        print(tweet)
        linecount += 1

print('LINES:', linecount)
