# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 17:20:43 2019

@author: User
"""

import statistics
from mrjob.job import MRJob
from mrjob.step import MRStep

class DegreeStats(MRJob):   
    def steps(self):
        return [MRStep(mapper=self.mapper_degree, reducer=self.reducer_degree),
                MRStep(mapper=self.mapper_stats, reducer=self.reducer_stats)]

    def mapper_degree(self, key, value):
        if "#" not in value and len(value.strip()) > 0:
            values = value.split()
            # Key = node, value = (no. of in-degrees, no. of out-degrees)
            yield int(values[0]), (0, 1)    # out-degree + 1
            yield int(values[1]), (1, 0)    # in-degree + 1
            
    def reducer_degree(self, key, values):
        li = list(values)
        l = [sum(x) for x in zip(*li)]
        yield key, (l[0],l[1])
        
    def mapper_stats(self, key, value):
        yield "in", value[0]
        yield "out", value[1]
        
    def reducer_stats(self, key, values):
        li = list(values)
        yield key + " avg", sum(li)/len(li)
        yield key + " median", statistics.median(li)
        
if __name__ == '__main__':
    DegreeStats.run()