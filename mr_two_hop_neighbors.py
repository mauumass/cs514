# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 18:57:10 2019

@author: User
"""

import statistics
import itertools
from mrjob.job import MRJob
from mrjob.step import MRStep

class TwoHopNeighbors(MRJob):    
    def steps(self):
       return [MRStep(mapper=self.mapper_edges, reducer=self.reducer_edges),
               MRStep(mapper=self.mapper_twohops, reducer=self.reducer_sum_two_hop_neighbors),
               MRStep(reducer=self.reducer_neighbors_stats)]
       
    def mapper_edges(self, key, value):
        if "#" not in value and len(value.strip()) > 0:
            values = value.split()
            yield int(values[0]), (int(values[0]), int(values[1]))
            yield int(values[1]), (int(values[0]), int(values[1]))
            
    def reducer_edges(self, key, values):            
        yield key, list(set([(x[0], x[1]) for x in values]))   # Remove duplicates
        
    def mapper_twohops(self, key, value):
        prev_neighbors = []
        one_hop_neighbors = []
        for v in value:            
            if v[0] != key:
                prev_neighbors.append(v[0])
            elif v[0] != v[1]: # Edges leading back to itself does not count
                one_hop_neighbors.append(v[1])
        for prev in prev_neighbors: # Notify prev neighbors of my 1 hop neighbor counts
            yield prev, one_hop_neighbors

        yield key, one_hop_neighbors
        
    def reducer_sum_two_hop_neighbors(self, key, values):
        s = set(list(itertools.chain.from_iterable(values)))
        if key in s:
            s.remove(key)
        yield "reachable two hop neighbor counts", len(s)
    
    def reducer_neighbors_stats(self, key, values):
        li = list(values)
        yield key + " avg", sum(li)/len(li)
        yield key + " median", statistics.median(li)
        
if __name__ == '__main__':
    TwoHopNeighbors.run()