# -*- coding:utf-8 -*-
__author__ = 'haven'
from bs4 import *
import requests
from urllib.parse import urljoin
import sqlite3
import re
import jieba

ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])
ignoreurls=set(['xml','atom'])

class crawler:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)
        self.cursor = self.con.cursor()

    def __del__(self):
        self.cursor.close()
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
        #splitter = re.compile(r'\W*')
        return jieba.lcut_for_search(text)
        #return [s.lower() for s in splitter.split(text) if s != '']

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
        sppage=set()
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
                            # 存档，友链
                            if re.search('archives|links|page',url):
                                sppage.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)
            self.dbcommit()
            pages = newpages
        self.crawl(sppage,5)

    def calculatepagerank(self, iteration=20):
        self.con.execute('DROP TABLE IF EXISTS pagerank')
        self.con.execute('CREATE TABLE pagerank(urlid PRIMARY KEY ,score)')

        self.con.execute('INSERT INTO pagerank SELECT ROWID,1.0 FROM urllist')
        self.dbcommit()

        for i in range(iteration):
            print('iteration %d' % i)
            for urlid, in self.con.execute('SELECT ROWID FROM urllist'):
                pr = 0.15
                for linker, in self.con.execute('SELECT DISTINCT fromid FROM link WHERE '
                                                'toid =?', (urlid,)):
                    linkingpr = self.con.execute('SELECT score FROM pagerank WHERE urlid=?', (linker,)).fetchone()[0]
                    linkcount = self.con.execute('SELECT count(*) FROM link WHERE fromid=?', (linker,)).fetchone()[0]

                    pr += 0.85 * (linkingpr / linkcount)
                self.con.execute('UPDATE pagerank SET score=? WHERE urlid=?', (pr, urlid))
            self.con.commit()

    def createindextables(self):
        self.con.execute('DROP TABLE IF EXISTS urllist')
        self.con.execute('DROP TABLE IF EXISTS wordlist')
        self.con.execute('DROP TABLE IF EXISTS wordlocation')
        self.con.execute('DROP TABLE IF EXISTS link')
        self.con.execute('DROP TABLE IF EXISTS linkwords')
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

        # 根据空格拆分query string
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

        fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return rows, wordids

    def getscoredlist(self, rows, wordids):
        totalscores = dict((row[0], 0) for row in rows)

        weights = [(1.0, self.locationscore(rows)), (1.0, self.frequencyscore(rows)), (1.0, self.pagerankscore(rows))]

        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]
        return totalscores

    def geturlname(self, id):
        return self.con.execute("SELECT url FROM urllist WHERE rowid=?", (id,)).fetchone()

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print('%f\t%s' % (score, self.geturlname(urlid)))

    def normalizescores(self, scores, smallIsBetter=False):
        vsmall = 0.0001
        if smallIsBetter:
            minscore = min(scores.values())
            return dict((u, minscore / max(c, vsmall)) for (u, c) in scores.items())
        else:
            maxscore = max(scores.values())
            if maxscore == 0: maxscore = vsmall
            return dict((u, c / maxscore) for (u, c) in scores.items())

    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows: counts[row[0]] += 1
        return self.normalizescores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            locsum = sum(row[1:])
            if locsum < 100000:
                locations[row[0]] = locsum

        return self.normalizescores(locations, smallIsBetter=True)

    def distancescore(self, rows):
        if len(rows[0]) < 2: return dict((row[0], 1.0) for row in rows)

        mindistance = dict((row[0], 1000000) for row in rows)

        for row in rows:
            distance = sum(row[i] - row[i - 1] for i in range(2, len(row)))
            if distance < 1000000: mindistance[row[0]] = distance
        return self.normalizescores(mindistance, smallIsBetter=True)

    def inboundlinkscore(self, rows):
        uniqueruls = set(row[0] for row in rows)
        inboundcount = dict(
            self.con.execute('SELECT count(*) FROM link WHERE toid=?;', (u,)).fetchone()[0] for u in uniqueruls)
        return self.normalizescores(inboundcount)

    def pagerankscore(self, rows):

        pageranks = dict(
            [(row[0], self.con.execute('SELECT score FROM pagerank WHERE urlid=?', (row[0],)).fetchone()[0]) for row in
             rows])
        return self.normalizescores(pageranks)
