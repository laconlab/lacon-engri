#!./venv/bin/python
import multiprocessing as mp
import sys
import os
import gzip
import json
import hashlib
import re

from datetime import datetime
from abc import ABC
from pathlib import Path
from typing import Generator, Tuple, Dict
from dataclasses import dataclass

from tqdm import tqdm
from bs4 import BeautifulSoup

DATE_FORMAT = "%Y/%m/%d"
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2024, 1, 1)

class Parser(ABC):
    def url(self, _: BeautifulSoup) -> str:
        raise NotImplemented
    def title(self, _: BeautifulSoup) -> str:
        raise NotImplemented
    def text(self, _: BeautifulSoup) -> str:
        raise NotImplemented
    def date(self, _: BeautifulSoup) -> str:
        raise NotImplemented


class HrtParser(Parser):
    def __init__(self):
        self.date_re = re.compile(r"\d{2}\.\d{2}\.(\d){4}\.")
    def url(self, soup: BeautifulSoup) -> str:
        ret = soup.find("meta", {"property": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("content")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        titles = soup.find_all("h1")
        assert len(titles) > 0, "unexpected number of titles found"
        return titles[0].getText()
    def date(self, soup: BeautifulSoup) -> str:
        dates = [self.date_re.match(p.get_text()) for p in soup.find_all("p")]
        dates = [date for date in dates if date is not None]
        assert len(dates) > 0, "unexpected number of dates"
        return datetime.strptime(dates[0].string, "%d.%m.%Y.").strftime(DATE_FORMAT)
    def text(self, soup: BeautifulSoup) -> str:
        ret = "\n\n".join([d.get_text() for d in soup.find_all("div") if "articleText" in d.get("class", [])])
        assert len(ret) > 10, "text to short"
        return ret

class DirektnoParser(Parser):
    def __init__(self):
        self.REMOVE_TAGS = ["blockquote", "script", "iframe", "span",
               "a", "em", "style", "figure", "g", "img",
               "path", "sup", "svg", "time"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS: 
            for it in html(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> str:
        url = soup.find("meta", {"property": "og:url"})
        assert url is not None, "cannot find url"
        url = url.get("content")
        assert url is not None, "cannot find url"
        return url.strip()
    def title(self, soup: BeautifulSoup) -> str:
        titles = soup.find_all("h1")
        assert len(titles) >= 1, f"unexpected number of titles found {titles}"
        return titles[0].getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        dates = [s.get("content") for s in soup.find_all("meta", {"property": "article:published_time"})]
        dates = [date for date in dates if date is not None]
        assert len(dates) == 1, f"unexpected number of dates {dates}"
        return datetime.strptime(dates[0].strip(), "%Y-%m-%d").strftime(DATE_FORMAT)
    def text(self, soup: BeautifulSoup) -> str:
        main = soup.find_all("div", {"class": "main-content"})
        assert len(main) == 1, "unexpected len of main content"
        main = main[0]
        self.remove_tags(main)
        content = main.find_all("p", attrs=lambda x: not x)
        assert len(content) > 0, "no text found"
        return "\n".join([c.getText().strip() for c in content])

class VecernjiParser(Parser):
    def __init__(self):
        self.date_re = re.compile(r"\d{4}\-\d{2}\-(\d){2}")
        self.REMOVE_TAGS = ["blockquote", "script", "iframe", "span",
               "a", "em", "style", "figure", "g", "img",
               "path", "sup", "svg", "time"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS: 
            for it in html(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> str:
        url = soup.find("meta", {"property": "og:url"})
        assert url is not None, "cannot find url"
        url = url.get("content")
        assert url is not None, "cannot find url"
        return url.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        assert title is not None, "title not found"
        return title.getText()
    def date(self, soup: BeautifulSoup) -> str:
        dates = [m for m in soup.find_all("meta") if m.get("itemprop", "") == "datePublished"]
        assert len(dates) == 1, f'unexpedted numbed of dates {len(dates)}'
        date = dates[0].get("content").strip()
        assert self.date_re.match(date) is not None, "date not found"
        return datetime.strptime(date, "%Y-%m-%d").strftime(DATE_FORMAT)
    def text(self, soup: BeautifulSoup) -> str:
        articles = [a for a in soup.find_all("article") if "single-article" in a.get("class", [])]
        assert len(articles) > 0, "cannot find article"
        for a in articles: self.remove_tags(a)
        ps = [p for a in articles for p in a.find_all("p")]
        assert len(ps) > 0, "cannot find paragraphs"
        ret = "\n\n".join([p.get_text() for p in ps if not p.find("div")])
        assert len(ret) > 10, "text too short"
        return ret

PARSER_MAP = {
    "hrt": HrtParser(),
    "direktno": DirektnoParser(),
    #"vecernji": VecernjiParser(),
}

@dataclass
class File:
    src_path: Path
    dst_path: Path
    parser: Parser


def get_files(src: Path, dst: Path) -> Tuple[Generator[File, File, None], int]:
    file_count = sum(len(os.listdir(src / f)) for f in os.listdir(src) if (src / f).is_dir())
    def gen():
        for folder in os.listdir(src):
            print(folder)
            if folder not in PARSER_MAP: continue
            if not (src / folder).is_dir(): continue
            for file in os.listdir(src / folder):
                if not file.endswith(".gz"): continue
                yield File(src / folder / file, dst / folder, PARSER_MAP[folder])
    return gen(), file_count


def load_json(path: Path) -> Tuple[Dict[str, str], BeautifulSoup]:
    with gzip.open(path, "rt", encoding="UTF-8") as f:
        data = json.load(f)
        html = BeautifulSoup(data["content"], "html.parser")
        del data["content"]
    return data, html


def process(file: File) -> None:
    try:
        data, html = load_json(file.src_path)
        data["url"] = file.parser.url(html)
        data["title"] = file.parser.title(html)
        data["text"] = file.parser.text(html)
        data["publish_date"] = file.parser.date(html)

        if not (START_DATE <= datetime.strptime(data["publish_date"], DATE_FORMAT) <= END_DATE):
            return

        save_path = file.dst_path
        for part in data["publish_date"].split("/"): save_path = save_path / part
        save_path = save_path / f"{hashlib.sha1(data['url'].encode()).hexdigest()}.json"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"{file=} -> {e}")


if __name__ == "__main__":
    _, src_root, dst_root = sys.argv
    files, file_count = get_files(Path(src_root), Path(dst_root))
    with mp.Pool(mp.cpu_count() * 2) as p:
        for _ in tqdm(p.imap(process, files), total=file_count):
            pass
