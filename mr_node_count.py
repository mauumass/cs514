# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 16:52:37 2019

@author: User
"""

from mrjob.job import MRJob

class NodeCount(MRJob):
    def mapper(self, key, value):
        if "#" not in value and len(value.strip()) > 0:
            values = value.split()
            yield 'node count', int(values[0])
            yield 'node count', int(values[1])
            
    def reducer(self, key, values):
        yield key, len(set(values))
        
if __name__ == '__main__':
    NodeCount.run()