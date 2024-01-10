#!./venv/bin/python
import multiprocessing as mp, sys, os, csv, json

from typing import Generator
from collections import Counter

import regex
import nltk
nltk.download('punkt')

from tqdm import tqdm
from nltk.tokenize import sent_tokenize

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


def get_files(src: str) -> Generator[str, None, None]:
    return (os.path.join(root, file) for root, _, files in os.walk(src) for file in files)

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

    # filter out empty words
    words = filter(lambda word: word, words)

    return Counter(words)


if __name__ == "__main__":
    _, src_root, dest_path = sys.argv
    print(f"processing from {src_root}")
    files = get_files(src_root)

    print("start processing")
    words = Counter()
    with mp.Pool(mp.cpu_count()) as p:
        for ws in tqdm(p.imap(process, files)):
            words.update(ws)

    print(f"writing csv file into {dest_path} with {len(words)} rows")
    with open(f"{dest_path}/word_list.csv", "w", encoding="utf8") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(["word", "frequency"])
        writer.writerows(words.items())

