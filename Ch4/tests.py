# -*- coding:utf-8 -*-
__author__ = 'haven'


from Ch4.searchengine import crawler

# crawler('searchindex.db').createoindextables()
craw= crawler('searchindex.db')
pages =['http://leetcode.com/']
craw.crawl(pages)
