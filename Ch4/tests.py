# -*- coding:utf-8 -*-
__author__ = 'haven'


from Ch4.searchengine import crawler,searcher

# crawler('searchindex.db').createindextables()


craw= crawler('searchindex.db')
craw.createindextables()
pages =['http://codepub.cn/']
craw.crawl(pages)
# craw.calculatepagerank()

# e = searcher('searchindex.db')
# e.query('反射')
# print(e.geturlname(5))
