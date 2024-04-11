## HTML -> json(texts)

``` bash
./parse.py <src-path> <dest-path>
```

## json(texts) -> word:frequency
``` bash
./wordy.py <src-path>
```

## word:frequency -> only engl word:frequency
``` bash
./classy.py
```

## archive raw htmls for only jsons in current corpus
``` bash
./archive.py <json-src-path> <html-src-path> <archive-dest-path>
```

## convert jsons to single txt file with only texts
``` bash
./txt_corpus.py <json-src-path> <txt-dest-path>
```
