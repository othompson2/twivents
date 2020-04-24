from datetime import datetime
import pytz
import numpy as np

def format_timestamp(created_at):
    return datetime.strptime(created_at,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)

def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))