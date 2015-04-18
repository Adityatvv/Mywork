#!/usr/bin/env python
#-*- coding:utf-8 -*-
from itertools import chain
import multiprocessing.dummy
p = multiprocessing.dummy.Pool(4)

# Input data
x=['x','y','x','z']
y=[['a','b','c'],['a','d','e'],['b','c','f','g'],['a','b']]

# Variable Declaration
i=0
index = {}
index1 = {}

# Phase I
# first stage Mapper emits dummy counters for each pair of F and G; 
# Reducer calculates a total number of occurrences for each such pair.
def mappera(y):
	global i
	pairs = []
	for block in y:	
		for c in block:
			pairs.append(((x[i],c),1))		
	i = i + 1	
	return pairs

def reducera(pairs2):
	pairs = []
	for (key,value) in pairs2:
		pairs.append((key,0))
	return pairs

# Phase II
# second phase pairs are grouped by G and the total number of items 
#					in each group is calculated.
def mapperb(pairs):
	final = []
	global index
	for (key,value) in pairs:
		buf = [key]
		for (xval,yval) in buf:
			if not index.has_key(key):	
				index[key] = xval
				final.append((yval,1))
	return final

def reducerb(pairs):
	global index1
	for (key,value) in pairs:		
		if not index1.has_key(key):
			index1[key] = value
		else:
			index1[key] = index1[key] + value
	final = []
	for key in index1:
		final.append((key,index1[key]))
	return final


# First pahse Mapper ( combiners two groups )
data = p.map(mappera,y)

# First phase reducer ( appends dummy counter )
data = p.map(reducera,data)

# Second phase mapper ( nullfy duplicate values )
data = p.map(mapperb,data)

# Second phase recuder (counts the appropriate values )
data = p.map(reducerb,data)

# Displays the last list of array 
print data[-1]
