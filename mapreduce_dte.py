#!/usr/bin/env python
#-*- coding:utf-8 -*-
# Two Basic map reduce patterns
# Distributed Task Execution
#    --  Distributed the long text into parts using " :: "
# Filtering (“Grepping”), Parsing, and Validation
#    --  Parsing the "\n" junk values into meaning full values	
import multiprocessing.dummy
import operator
from operator import itemgetter
p = multiprocessing.dummy.Pool(6)

x="""
adksfjdhglrtawjdfkslj
dfsdsgafgdgwwwdvs
jfsdjkughsdksjhfkljdh
fkjsdkfjwlghblkdjhsdf
jksdfhkjfkldjfhl
akjhalkjswfwrwfwggfsgf::
lkdfjgoksdjsfhskjdf
jkdkbvsfwffjwwiueiw
ldkflnornnfowanejb
apajgjegrjrgfgfasgas::
sdfdfgfdgfdgdfgsg
sdfdfsdf
sgghgfhddhdhdhfjarwffgd
ddhggehefhdfhdh::
dgdhedfsfgdhdhgh
dhsgsfeshtdfdbhsghsdhsdhg::
dfgdfgeefdgfdhdghdsh
dhdhshsergefhghsfh
"""

# Distributed Task Execution
x = x.strip().split('::')

def mapper(s): # string -> [(key value)]
	pairs = []
	for c in s:
		#Filtering (“Grepping”), Parsing, and Validation
		if c == "\n":
	 		c = c.replace(str(c),"New Line")
		pairs.append((c,1))
	return pairs

def combiner(pairs):
	index = {}
	for (key,value) in pairs:
		if not index.has_key(key):
			index[key] = value
		else:
			index[key] = index[key] + value
	pairs = []
	for key in index:
		pairs.append((key,index[key]))
	return pairs

def reducer(data):
	index = {}
	for pairs in data:
		for (key,value) in pairs:
			if not index.has_key(key):
				index[key] = value
			else:
				index[key] = index[key] + value

	pairs = []    
	for key in  sorted(index,reverse = True):
		pairs.append((key,index[key]))
	return pairs

data = p.map(mapper,x)

data = p.map(combiner,data)

data = reducer(data)

# Display output
print data
