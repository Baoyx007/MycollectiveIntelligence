# -*- coding:utf-8 -*-
__author__ = 'haven'


from Ch4.searchengine import crawler,searcher

# crawler('searchindex.db').createindextables()


# craw= crawler('searchindex.db')
# craw.calculatepagerank(gj)
# # pages =['http://leetcode.com/','https://docs.python.org/3/library/']
# craw.crawl(pages)

e = searcher('searchindex.db')
# print(e.getmatchrows('example programming'))


e.query('functional programming')
# print(e.geturlname(5))
