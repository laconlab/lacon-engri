#!./venv/bin/python
import multiprocessing as mp
import sys
import os
import json
import re

from typing import List, Dict
from collections import Counter

import nltk
nltk.download('punkt')

from tqdm import tqdm
from nltk.tokenize import sent_tokenize

RE_URL = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
TRANSLATE = str.maketrans('', '', ''',.?;:'"!()+=/''')
VALID_CHARS = set('''qwertyuiopasdfghjklzxcvbnmčćđšž,.?;:'"!()-+=/''')


def get_files(src: str) -> List[str]:
    return [os.path.join(root, file) for root, _, files in os.walk(src) for file in files]

def process(file_path: str):
    with open(file_path, "r") as f:
        data = json.load(f)

    ret = text_to_words(data["text"])
    ret.update(text_to_words(data["title"]))
    return ret

def text_to_words(text: str):
    # split to sentences
    sentences = sent_tokenize(text)

    # split by space
    words = (w for s in sentences for w in s.split(" "))

    # filter out urls
    words = filter(lambda word: RE_URL.match(word) is None, words)

    # filter out capitalized words
    words = filter(lambda word: not word.istitle(), words)

    # replace - from front and back if exists
    words = map(lambda word: word.strip("-"), words)

    # filter double or more '-'
    words = filter(lambda word: Counter(word)["-"] <= 1, words)

    # replace all symbols
    words = map(lambda word: word.translate(TRANSLATE), words)

    # to lowercase
    words = map(lambda word: word.lower(), words)

    # take only if word has valid chars
    words = filter(lambda word: len(set(word) - VALID_CHARS) == 0, words)

    return Counter(words)


if __name__ == "__main__":
    _, src_root = sys.argv
    files = get_files(src_root)
    words = Counter()
    with mp.Pool(mp.cpu_count()) as p:
        for ws in tqdm(p.map(process, files), total=len(files)):
            words.update(ws)
    with open("words.json", "w", encoding='utf8') as f:
        json.dump(words, f, ensure_ascii=False)

