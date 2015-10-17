from math import sqrt
__author__ = 'haven'

def readfile(filename):
    lines = [line for line in open(filename)]
    colnames = lines[0].strip().split('\t')[1:]
    rownames=[]
    data=[]

    for line in lines[1:]:
        p = line.strip().split('\t')
        rownames.append(p[0])
        data.append([float(x) for x in p[1:]])
    return rownames,colnames,data


# print(readfile('blogdata.txt'))

def pearson(v1,v2):
    sum1=sum(v1)
    sum2=sum(v2)

    sum1Sq=sum([pow(v,2) for v in v1])
    sum2Sq=sum([pow(v,2) for v in v2])


