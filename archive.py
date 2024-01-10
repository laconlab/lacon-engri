#!./venv/bin/python

import sys, os, json, gzip, subprocess
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

if __name__ == "__main__":
    _, parsed_src, og_src, archive_dest = map(Path, sys.argv)
    print(f"{parsed_src=}, {og_src=}, {archive_dest=}")

    websites = os.listdir(parsed_src)
    for website in map(Path, websites):
        path = parsed_src / website
        for year in os.listdir(path):
            print(f"copy {website} {year}")
            path = path / Path(year)
            for path, _, files in os.walk(path):
                for file in files:
                    src_parse_path = Path(path) / file
                    with open(src_parse_path, "rb") as f:
                        id = json.load(f)["id"]
                    
                    src_og_path = Path(og_src) / website / f"{id}.gz"
                    with gzip.open(src_og_path, "rb") as f:
                        data = f.read()

                    save_path = Path(archive_dest) / website / year / f"{id}.json"
                    os.makedirs(save_path.parent, exist_ok=True)
                    with open(save_path, "wb") as f:
                        f.write(data)

            print(f"compressing {website} {year}")
            compress_path = Path(archive_dest) / website / year
            subprocess.run(["tar", "-cJf", f"{compress_path}.tar.xz", compress_path])

            print(f"deleting {website} {year}")
            subprocess.run(["rm", "-rf", compress_path])

