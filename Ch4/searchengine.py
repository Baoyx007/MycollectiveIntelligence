# -*- coding:utf-8 -*-
__author__ = 'haven'
from bs4 import *
import requests
from urllib.parse import urljoin
import sqlite3

ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])


class crawler:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def getentryid(self, table, field, value, createnew=True):
        return None

    def addtoindex(self, url, soup):
        print('indexing %s' % url)

    def gettextonly(self, soup):
        return None

    def separatewords(self, text):
        return None

    def isindexed(self, url):
        return False

    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

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
        self.con.execute('create tabel urllist(url)')
        self.con.execute('create tabel urllist(url)')
        self.con.execute('create tabel urllist(url)')
        self.con.execute('create tabel urllist(url)')
        self.con.execute('create tabel urllist(url)')

        # crawler('').crawl(['http://docs.python-requests.org/en/latest/user/quickstart/'])
