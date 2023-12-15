#!./venv/bin/python
import multiprocessing as mp
import sys
import os
import json

from typing import List
from collections import Counter

import nltk
nltk.download('punkt')

from tqdm import tqdm
from nltk.tokenize import sent_tokenize
import regex

RE_URL = regex.compile(r"^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$", regex.IGNORECASE)
def is_url(word: str) -> bool:
    try:
        return RE_URL.match(word, timeout=1) is not None
    except Exception:
        return False
RE_EMAIL = regex.compile(r"^((?!\.)[\w\-_.]*[^.])(@\w+)(\.\w+(\.\w+)?[^.\W])$", regex.IGNORECASE)
def is_email(word: str) -> bool:
    try:
        return RE_EMAIL.match(word, timeout=1) is not None
    except Exception:
        return False
TRANSLATE = str.maketrans('', '', ''',.?;:'"!()+=/»’“''')
VALID_CHARS = set('''qwertyuiopasdfghjklzxcvbnmčćđšž,.?;:'"!()-+=/''')


def get_files(src: str) -> List[str]:
    return [os.path.join(root, file) for root, _, files in os.walk(src) for file in files]

def process(file_path: str):
    with open(file_path, "r") as f:
        data = json.load(f)

    # split to sentences
    sentences = sent_tokenize(data["text"])

    # split by space
    words = (w for s in sentences for w in s.split(" "))

    # filter out urls
    words = filter(lambda word: not is_url(word), words)

    # filter out emails
    words = filter(lambda word: not is_email(word), words)

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
        for ws in tqdm(p.imap(process, files), total=len(files)):
            words.update(ws)
    with open("words.json", "w", encoding="utf8") as f:
        json.dump(words, f, ensure_ascii=False)

