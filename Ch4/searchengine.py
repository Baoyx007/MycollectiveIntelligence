# -*- coding:utf-8 -*-
__author__ = 'haven'
from bs4 import *
import requests
from urllib.parse import urljoin
import sqlite3
import re

ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])


class crawler:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute('SELECT ROWID FROM %s WHERE %s=?' % (table, field), (value,))
        res = cur.fetchone()
        if not res:
            cur = self.con.execute('INSERT INTO %s (%s) VALUES (?)' % (table, field), (value,))
            return cur.lastrowid
        else:
            return res[0]

    def addtoindex(self, url, soup):
        if self.isindexed(url): return
        print('indexing %s' % url)

        text = self.gettextonly(soup)
        words = self.separatewords(text)

        urlid = self.getentryid('urllist', 'url', url)

        for i, word in enumerate(words):
            if word in ignorewords: continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute('INSERT INTO wordlocation(urlid,wordid,location)'
                             'VALUES (?,?,?)', (urlid, wordid, i))

    def gettextonly(self, soup):
        v = soup.string
        if not v:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext
            return resulttext
        else:
            return v.strip()

    def separatewords(self, text):
        splitter = re.compile(r'\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    def isindexed(self, url):
        u = self.con.execute('SELECT ROWID FROM urllist WHERE url=?', (url,)).fetchone()
        if u:
            v = self.con.execute('SELECT * FROM wordlocation WHERE urlid=?', (u[0],)).fetchone()
            if not v: return True
        return False

    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    # test
    # crawler('').crawl(['http://docs.python-requests.org/en/latest/user/quickstart/'])
    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    # slow io
                    c = requests.get(page)
                except BaseException:
                    print('could not open %s' % page)
                    continue

                # slow parse
                soup = BeautifulSoup(c.text, 'html.parser')
                self.addtoindex(page, soup)
                links = soup('a')
                for link in links:
                    if 'href' in link.attrs:
                        url = urljoin(page, link['href'])
                        if url.find('\'') != -1: continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)
            self.dbcommit()
            pages = newpages

    def createindextables(self):
        self.con.execute('CREATE TABLE urllist(url)')
        self.con.execute('CREATE TABLE wordlist(word)')
        self.con.execute('CREATE TABLE wordlocation(urlid,wordid,location)')
        self.con.execute('CREATE TABLE link(fromid INTEGER,toid INTEGER)')
        self.con.execute('CREATE TABLE linkwords(wordid,linkid)')
        self.con.execute('CREATE INDEX wordiddx ON wordlist(word)')
        self.con.execute('CREATE INDEX urlidx ON urllist(url)')
        self.con.execute('CREATE INDEX wordurlidx ON wordlocation(wordid)')
        self.con.execute('CREATE INDEX urltoidx ON link(toid)')
        self.con.execute('CREATE INDEX urlfromidx ON link(fromid)')


class searcher:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    # q is querying string
    def getmatchrows(self, q):
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        #根据空格拆分query string
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            wordrow = self.con.execute('SELECT ROWID FROM wordlist WHERE word=?', (word,)).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (tablenumber - 1, tablenumber)

                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1

        fullquery='select %s from %s where %s'%(fieldlist,tablelist,clauselist)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return  rows,wordids
