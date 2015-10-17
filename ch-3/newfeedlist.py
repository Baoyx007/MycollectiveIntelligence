__author__ = 'haven'
list = [line for line in open('feedlist.txt') if line.endswith('xml\n')]

# print(list)
file = open('feedlist2.txt', 'w')
for line in list:
    file.write(line)
