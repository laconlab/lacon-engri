#!./venv/bin/python

import sys, os, json, gzip, subprocess, multiprocessing as mp
from pathlib import Path

# make sure you do this for each year
# 1. take processing files
# 2. find ids
# 3. find original json assosiated with such id
# 4. save uncompressed json in archive path
#   path -> <site>/<year>
# 5. use xz compression for each year
# 6. delete uncompressed version of a file

# only when everything is done manully delete original gziped jsons

def process(args):
    website, year, path = args
    print(f"copy {website} {year} {path}")
    file_count = 0
    for path, _, files in os.walk(path):
        for file in files:
            src_parse_path = Path(path) / file
            with open(src_parse_path, "rb") as f:
                id = json.load(f)["id"]

            src_og_path = Path(og_src) / website / f"{id}.gz"
            with gzip.open(src_og_path, "rb") as f:
                data = f.read()

            save_path = Path(archive_dest) / website / year / f"{id}.json"
            with open(save_path, "wb") as f:
                f.write(data)
            file_count += 1
    print(f"compressing {website} {year} {file_count}")
    compress_path = Path(archive_dest) / website / year
    subprocess.run(["tar", "-cJf", f"{compress_path}.tar.xz", compress_path])

    print(f"deleting {website} {year}")
    subprocess.run(["rm", "-rf", compress_path])

if __name__ == "__main__":
    _, parsed_src, og_src, archive_dest = map(Path, sys.argv)
    print(f"{parsed_src=}, {og_src=}, {archive_dest=}")

    args = []
    websites = os.listdir(parsed_src)
    for website in map(Path, websites):
        path = parsed_src / website
        for year in os.listdir(path):
            save_path = Path(archive_dest) / website / year
            sp = Path(archive_dest) / website / f"{year}.tar.xz"
            if not save_path.exists() and sp.exists():
                continue
            os.makedirs(save_path, exist_ok=True)
            args.append((website, year, path / Path(year)))

    with mp.Pool(mp.cpu_count() * 4) as pool:
        pool.map(process, args)
