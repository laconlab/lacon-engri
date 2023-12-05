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
from typing import Generator, Tuple, Dict, Optional
from dataclasses import dataclass

from tqdm import tqdm
from bs4 import BeautifulSoup

DATE_FORMAT = "%Y/%m/%d"
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2024, 1, 1)
DATE_MAP = {"siječnja": 1, "siječanj": 1, "veljače": 2, "veljača": 2, "ožujka": 3, "ožujak": 3, "travnja": 4, "travanj": 4,
            "svibnja": 5, "svibanj": 5, "lipnja": 6, "lipanj": 6, "srpnja": 7, "srpanj": 7, "kolovoza": 8, "kolovoz": 8,
            "rujna": 9, "rujan": 9, "listopada": 10, "listopad": 10, "studenog": 11, "studeni": 11, "studenoga": 11,
            "prosinca": 12, "prosinac": 12}

class Parser(ABC):
    def url(self, _: BeautifulSoup) -> Optional[str]:
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
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        ret = soup.find("meta", {"property": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("content")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
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
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        url = soup.find("meta", {"property": "og:url"})
        assert url is not None, "cannot find url"
        url = url.get("content")
        assert url is not None, "cannot find url"
        return url.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
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
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        url = soup.find("meta", {"property": "og:url"})
        assert url is not None, "cannot find url"
        url = url.get("content")
        assert url is not None, "cannot find url"
        return url.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
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

class NoviListParser(Parser):
    REMOVE_TAGS = ["blockquote", "script", "iframe", "span",
                    "a", "em", "style", "figure", "g", "img",
                    "path", "sup", "svg", "time", "strong", "video", "font", "div"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        url = soup.find("meta", {"property": "og:url"})
        assert url is not None, "cannot find url"
        url = url.get("content")
        assert url is not None, "cannot find url"
        url = url.strip()
        if url == """https://www.novilist.hr/novosti/""": return None
        return url
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1", {"class": "article-title"})
        return "" if title is None else title.getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        dates = soup.find_all("meta", {"property": "article:published_time"})
        assert len(dates) == 1, f'unexpedted numbed of dates {len(dates)}'
        date = dates[0].get("content").strip()
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z").strftime(DATE_FORMAT)
    def text(self, soup: BeautifulSoup) -> str:
        intro = soup.find("p", {"class": "intro-text"})
        intro = "" if intro is None else intro.get_text().strip()
        article = soup.find("div", {"class": "user-content"})
        assert article is not None and len(article) > 0, "cannot find article"
        self.remove_tags(article)
        text = article.get_text().strip()
        text = "\n".join((intro, text)).strip()
        assert len(text) > 0, "text content not found"
        return text

class Sata24Parser(Parser):
    REMOVE_TAGS = ["a", "script", "blockquote", "iframe", "em", "styple", "source", "video-js", "img", "span",
                   "input", "ul", "figure"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        url = soup.find("meta", {"property": "og:url"})
        assert url is not None, "cannot find url"
        url = url.get("content")
        assert url is not None, "cannot find url"
        return url.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        date = soup.find("time", {"class": "article__time"})
        assert date is not None, "cannot find date"
        date = date.get("datetime")
        assert date is not None, "cannot find date"
        return datetime.strptime(date, "%Y-%m-%d").strftime(DATE_FORMAT)
    def text(self, soup: BeautifulSoup) -> str:
        article = soup.find("div", {"class": "article__body"})
        assert article is not None, "cannot find article"
        self.remove_tags(article)
        ret =  article.getText().strip()
        assert len(ret) > 0, "cannot find article content"
        return ret

class DnevnoParser(Parser):
    REMOVE_TAGS = ["div", "img", "a", "h1", "style", "time", "script", "blockquote",
                   "ins", "h3", "article", "video", "em", "iframe", "img"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        ret = soup.find("meta", {"property": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("content")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
    def text(self, soup: BeautifulSoup) -> str:
        text = soup.find("div", {"id": "content"})
        assert text is not None, "text not found"
        self.remove_tags(text)
        return text.getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        date = soup.find("time", {"class": "date"})
        assert date is not None, "cannot find date"
        return datetime.strptime(date.get("datetime"), "%Y-%m-%d").strftime(DATE_FORMAT)

class SlobodnaParser(Parser):
    REMOVE_TAGS = ["a", "figure", "div", "script", "blockquote", "em", "style", "video", "img", "iframe"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        ret = soup.find("meta", {"property": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("content")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
    def text(self, soup: BeautifulSoup) -> str:
        text = soup.find("div", {"class": "itemFullText"})
        assert text is not None, "text not found"
        self.remove_tags(text)
        return text.getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        date = soup.find("div", {"class": "item__dates"})
        assert date is not None, "cannot find date"
        date = date.getText().split("-")[0].strip()
        day, month, year = date.replace(".", "").split(" ")
        return f"{year}/{DATE_MAP[month]}/{day}"

class IndexhrParser(Parser):
    REMOVE_TAGS = ["script", "span", "img", "a", "i", "blockquote", "iframe", "em", "style", "video", "font"]
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        ret = soup.find("link", {"rel": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("href")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        return soup.find("h1", {"class": "title"}).getText().strip()
    def text(self, soup: BeautifulSoup) -> str:
        text = soup.find("div", {"class": "content-holder"})
        assert text is not None, "text not found"
        self.remove_tags(text)
        return text.getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        date = soup.find("div", {"class": "article-info"}).getText().strip()
        dates = re.findall(r"\d+\.\s.*\s\d+.", date)
        assert len(dates) >= 1, "date not found"
        day, month, year = dates[0].replace(".", "").split(" ")
        return f"{year}/{DATE_MAP[month]}/{day}"

class JutarnjiParser(Parser):
    REMOVE_TAGS = ["a", "blockquote", "script", "iframe", "em", "style", "video", "img"]
    DATE_RE = re.compile(r"\d+\.\s.*\s\d+\.")
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        ret = soup.find("meta", {"property": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("content")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1", {"class": "item__title"})
        return "" if title is None else title.getText().strip()
    def text(self, soup: BeautifulSoup) -> str:
        text = soup.find("div", {"class": "itemFullText"})
        assert text is not None, "cannot find text"
        self.remove_tags(text)
        ps = text.find_all("p")
        assert len(ps) > 0, "cannot find paragraphs"
        return "\n".join([p.getText().strip() for p in ps])
    def date(self, soup: BeautifulSoup) -> str:
        date = soup.find("span", {"class": "item__author__date"})
        assert date is not None, "cannot find date"
        date =date.getText().strip()
        dates = self.DATE_RE.findall(date)
        assert len(dates) == 1, f"unexpected number of dates {dates}"
        day, month, year = dates[0].replace(".", "").split(" ")
        return f"{year}/{DATE_MAP[month]}/{day}"

class TelegramParser(Parser):
    REMOVE_TAGS = ["a", "span", "blockquote", "script", "iframe", "em", "style", "video", "img"]
    DATE_RE = re.compile(r"\d+\.\s.*\s\d+\.")
    def remove_tags(self, html: BeautifulSoup) -> None:
        for tag in self.REMOVE_TAGS:
            for it in html.find_all(tag): it.extract()
    def url(self, soup: BeautifulSoup) -> Optional[str]:
        ret = soup.find("meta", {"property": "og:url"})
        assert ret is not None, "cannot find url"
        ret = ret.get("content")
        assert ret is not None, "cannot find url"
        return ret.strip()
    def title(self, soup: BeautifulSoup) -> str:
        title = soup.find("h1")
        return "" if title is None else title.getText().strip()
    def text(self, soup: BeautifulSoup) -> str:
        text = soup.find("div", {"id": "article-content"})
        assert text is not None, "cannot find text"
        self.remove_tags(text)
        return text.getText().strip()
    def date(self, soup: BeautifulSoup) -> str:
        date = soup.find("span", {"class": "meta-date"})
        assert date is not None, "cannot find date"
        return datetime.strptime(date.getText().strip(), "%d. %m. %Y.").strftime(DATE_FORMAT)

PARSER_MAP = {
    #"hrt": HrtParser(),
    #"direktno": DirektnoParser(),
    #"vecernji": VecernjiParser(),
    #"novilist": NoviListParser(),
    #"24sata": Sata24Parser(),
    #"dnevno": DnevnoParser(),
    #"slobodnadalmacija": SlobodnaParser(),
    #"indexhr": IndexhrParser(),
    #"jutarnji": JutarnjiParser(),
    "telegram": TelegramParser(),
}

@dataclass
class File:
    src_path: Path
    dst_path: Path
    parser: Parser


def get_files(src: Path, dst: Path) -> Tuple[Generator[File, File, None], int]:
    file_count = sum(len(os.listdir(src / f)) for f in os.listdir(src) if (src / f).is_dir() and f in PARSER_MAP)
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

        url = file.parser.url(html)
        if url is None:
            return
        data["url"] = url
        data["publish_date"] = file.parser.date(html)
        if not (START_DATE <= datetime.strptime(data["publish_date"], DATE_FORMAT) <= END_DATE):
            return

        title = file.parser.title(html)
        text = "\n".join([title, file.parser.text(html)]).strip()
        assert len(text) > 0, "text too short"
        data["text"] = text

        save_path = file.dst_path
        for part in data["publish_date"].split("/"): save_path = save_path / part
        save_path = save_path / f"{hashlib.sha1(data['url'].encode()).hexdigest()}.json"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        with open(save_path, "w", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        print(f"{file.src_path=} -> {e}")


if __name__ == "__main__":
    _, src_root, dst_root = sys.argv
    files, file_count = get_files(Path(src_root), Path(dst_root))
    with mp.Pool(mp.cpu_count() * 2) as p:
        for _ in tqdm(p.imap(process, files), total=file_count):
            pass
