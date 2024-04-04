#!./venv/bin/python
import multiprocessing as mp, sys, os, json, gzip
from typing import Generator
from tqdm import tqdm

def get_files(src: str) -> Generator[str, None, None]:
    return (os.path.join(root, file) for root, _, files in os.walk(src) for file in files if file.endswith(".json"))

def load_txt(path: str) -> str:
    with open(path, "rb") as f:
        return json.load(f)["text"].strip() + "\n\n"

def process(src_root: str, dest: str) -> None:
    files = get_files(src_root)
    with gzip.open(dest, "wt", compresslevel=9) as o:
        with mp.Pool(mp.cpu_count()) as p:
            for data in tqdm(p.imap(load_txt, files)):
                o.write(data)

if __name__ == "__main__":
    _, src_root, dest_path = sys.argv
    print(f"processing from {src_root}")
    process(src_root, dest_path)

