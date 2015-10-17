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
    txt = re.compile(r'<[^>]+>').sub('', html)
    # print(txt)
    words = re.compile(r'[^A-Z^a-z]+').split(txt)

    return [word.lower() for word in words if word != '']


apcount = {}
wordcounts = {}
feedlist = [line for line in open('feedlist2.txt')]
# print(feedlist)
for feedurl in feedlist:
    title, wc = getwordcounts(feedurl)
    wordcounts[title] = wc
    for word, count in wc.items():
        apcount.setdefault(word, 0)
        if count > 1:
            apcount[word] += 1

    print(apcount)
