"""
# Automatic mode
treshold     = 0.70
minGroupSize = 1

from jellyfish import jaro_similarity
from itertools import combinations

paired = { c:{c} for c in data }
for a,b in combinations(data,2):
    if jaro_similarity(a,b) < treshold: continue
    paired[a].add(b)
    paired[b].add(a)

groups    = list()
ungrouped = set(data)
while ungrouped:
    bestGroup = {}
    for city in ungrouped:
        g = paired[city] & ungrouped
        for c in g.copy():
            g &= paired[c] 
        if len(g) > len(bestGroup):
            bestGroup = g
    if len(bestGroup) < minGroupSize : break  # to terminate grouping early change minGroupSize to 3
    ungrouped -= bestGroup
    groups.append(bestGroup)
"""
