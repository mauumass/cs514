# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 20:39:31 2019

@author: User
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

i = 100

class IndegreeCounts(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count),
                MRStep(reducer=self.reducer_aggregate)]
    
    def mapper_count(self, key, value):
        if "#" not in value and len(value.strip()) > 0:
            values = value.split()
            yield int(values[1]), 1
            
    def reducer_count(self, key, values):
        indegrees = sum(list(values))
        
        if indegrees > 100:
            yield "num of nodes > " + str(i) + " degrees", 1
            
    def reducer_aggregate(self, key, values):
        yield key, sum(list(values))
        
if __name__ == '__main__':
    IndegreeCounts.run()