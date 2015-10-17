import feedparser
import re

__author__ = 'haven'


def getwordcounts(url):
    d = feedparser.parse(url)
    wc = {}

    for e in d.entries:
        if 'summary' in e:
            summary = e.summary
        else:
            summary = e.description
        # print(summary)

        words = getwords(e.title + '' + summary)
        for word in words:
            wc.setdefault(word, 0)
            wc[word] += 1
    return d.feed.title, wc

def getwords(html):
    # print(html)
    txt = re.compile(r'<[^>]+>').sub('',html)
    # print(txt)
    words =re.compile(r'[^A-Z^a-z]+').split(txt)

    return [word.lower() for word in words if word != '']

t,w = getwordcounts('http://www.reasonsmysoniscrying.com/rss')
print(t,w)
