#!./venv/bin/python

# classify engl words from given word list
import sqlite3, time, sys, pickle, multiprocessing as mp
from typing import List, Optional


from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import download

from sklearn.pipeline import Pipeline

download("wordnet", quiet=True)
download("stopwords", quiet=True)
AEIOUY = set("aeiouy")
VOWELS = set("aeiou")
CACHE_DB = "cache.db"

with open(f"NGRAM_SVC.pkl", "rb") as f:
    model: Pipeline = pickle.load(f)


class Word:
    wordnet_lemmatizer = WordNetLemmatizer()
    def __init__(self, word: str, freq: int) -> None:
        self.word = word
        self.freq = freq
        self.wn_lemma = self.wordnet_lemmatizer.lemmatize(word)
        self.keep = False
        self.manual_lemma = None

    def lemma(self) -> str:
        return self.manual_lemma if self.manual_lemma else self.wn_lemma

def show_progress(func):
    def inner(*args, **kwargs):
        start = time.time()
        input_size = 0
        if len(args) > 0 and isinstance(args[0], list):
            input_size = len(args[0])
        res = func(*args, **kwargs)

        p = "" if input_size == 0 else f" (-{(1 - len(res) / input_size) * 100:.1f}%)"
        print(f"{func.__name__} took {time.time()-start:.3f}s to process from {input_size} to {len(res)} words{p}")
        return res
    return inner

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process(word_list_path: str):
    words = load_words(word_list_path)
    words = remove_short_words(words)
    words = remove_infrequent_words(words)
    words = remove_words_in_hml(words)
    words = remove_triple_letter_words(words)
    words = remove_words_without_aeiouy(words)
    words = remove_words_with_prefix(words, ["al-"])
    words = remove_words_with_suffix(words, ["hr", "com", "eu"])
    words = remove_words_with_double_vowel_prefix(words)
    words = remove_engl_stopwords(words)
    words = remove_using_manual_dataset(words)
    words = add_manual_lemma(words)
    words = remove_using_model(words)
    save(words, "engl_word_list.csv")

@show_progress
def load_words(path: str) -> List[Word]:
    words = []
    with open(path, "r") as f:
        for i, line in enumerate(f.readlines()):
            if i == 0 or not line: continue
            word, freq = line.strip().split(",")
            words.append(Word(word, int(freq)))
    return words

@show_progress
def remove_short_words(words: List[Word]) -> List[Word]:
    return [word for word in words if len(word.word) > 2]

@show_progress
def remove_infrequent_words(words: List[Word]) -> List[Word]:
    return [word for word in words if word.freq > 2]

@show_progress
def remove_triple_letter_words(words: List[Word]) -> List[Word]:
    return [word for word in words if not has_triple_letter(word.word)]

def has_triple_letter(word: str) -> bool:
    for i in range(len(word) - 2):
        if word[i] == word[i + 1] == word[i + 2]:
            return True
    return False

@show_progress
def remove_words_without_aeiouy(words: List[Word]) -> List[Word]:
    return [word for word in words if len(AEIOUY - set(word.word)) != 6]

@show_progress
def remove_words_with_prefix(words: List[Word], prefix:List[str]) -> List[Word]:
    return [word for word in words if not any(word.word.startswith(p) for p in prefix)]

@show_progress
def remove_words_with_suffix(words: List[Word], suffix:List[str]) -> List[Word]:
    return [word for word in words if not any(word.word.endswith(s) for s in suffix)]

@show_progress
def remove_words_with_double_vowel_prefix(words: List[Word]) -> List[Word]:
    return [word for word in words if not starts_with_double_vowel(word.word)]

def starts_with_double_vowel(word: str) -> bool:
    if word in {"aardvark","eerie","eek","eel","llama","ooh","oops","ooze","oozing"}: return False
    return len(word) > 2 and word[0] in VOWELS and word[1] in VOWELS

@show_progress
def remove_engl_stopwords(words: List[Word]) -> List[Word]:
    remove = set(stopwords.words())
    return [word for word in words if word.word not in remove]

@show_progress
def remove_words_in_hml(words: List[Word]) -> List[Word]:
    cache = load_hml_cache()
    to_check_hml_words = []
    ret = []
    for word in words:
        hml_found = cache.get(word.word)
        hml_wn_found = cache.get(word.wn_lemma)
        if hml_found is None: to_check_hml_words.append(word.word)
        elif hml_found: continue
        elif hml_wn_found is None: to_check_hml_words.append(word.wn_lemma)
        elif hml_wn_found: continue
        else: ret.append(word)

    if len(to_check_hml_words) > 0:
        for i, chunk in enumerate(chunks(to_check_hml_words, 49_000)):
            with open(f"check_hml_{i}.csv", "w") as f:
                f.write("\n".join(chunk))
        print(f"{len(to_check_hml_words)}/{len(words)} not found in hml cache, add them manully")
        sys.exit(1)
    return ret

def load_hml_cache():
    start = time.time()
    hml_cache = {}
    with sqlite3.connect(CACHE_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hml_cache (
                word TEXT UNIQUE,
                found BOOL
            );
        """)
        cursor.execute("SELECT word, found FROM hml_cache;")
        hml_cache = {word: found for word, found in cursor.fetchall()}
    print(f"hml cache load took {time.time() - start:.3f}s")
    return hml_cache

@show_progress
def remove_using_manual_dataset(words: List[Word]) -> List[Word]:
    cache = load_manual_class_cache()
    def any_in_cache(word: Word, tpe: int):
        return any(cache.get(w, -1) == tpe for w in [word.word, word.wn_lemma])
    ret = []
    for word in words:
        # in cache and non engl
        if any_in_cache(word, 0):
            continue

        # in cache and is engl
        if any_in_cache(word, 1):
            word.keep = True

        ret.append(word)
    return ret

def load_manual_class_cache():
    start = time.time()
    ret = {}
    with sqlite3.connect(CACHE_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_class_cache (
                word TEXT UNIQUE,
                is_engl BOOL
            );
        """)
        cursor.execute("SELECT word, is_engl FROM manual_class_cache;")
        ret = {word: int(is_engl) for word, is_engl in cursor.fetchall()}
    print(f"manual class cache load took {time.time() - start:.3f}s")
    return ret

def add_manual_lemma(words: List[Word]) -> List[Word]:
    cache = load_manual_lemmas_cache()
    for word in words:
        word.manual_lemma = cache.get(word.word)
    return words

def load_manual_lemmas_cache():
    start = time.time()
    ret = {}
    with sqlite3.connect(CACHE_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_lemma_cache (
                word TEXT UNIQUE,
                lemma TEXT UNIQUE
            );
        """)
        cursor.execute("SELECT word, lemma FROM manual_lemma_cache;")
        ret = {word: lemma for word, lemma in cursor.fetchall()}
    print(f"manual class cache load took {time.time() - start:.3f}s")
    return ret

@show_progress
def remove_using_model(words: List[Word]) -> List[Word]:
    import multiprocessing as mp
    with mp.Pool(mp.cpu_count()) as pool:
        res = pool.map(predict, words)
    return [r for r in res if r]

def predict(word: Word) -> Optional[Word]:
    global model
    if word.keep: return word
    elif model.predict([word.word]): return word
    elif model.predict([word.wn_lemma]): return word
    return None

def save(words: List[Word], path: str) -> None:
    words.sort(key=lambda w: w.lemma())
    with open(path, "w") as f:
        f.write("word,lemma,frequency\n")
        for word in words:
            f.write(f"{word.word},{word.lemma()},{word.freq}\n")
    print(f"saved {len(words)} words {len(set(w.lemma() for w in words))} lemmas in {path}")

if __name__ == "__main__":
    process("word_list.csv")
