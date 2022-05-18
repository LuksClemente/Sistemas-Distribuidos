import sys, os, time, math, re
from pyDF import *

sys.path.append(os.environ['PYDFHOME'])
	
regex = '(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})'

def filterSites(args):
	
	sp = args[0]

    if re.search(regex, sp):

        return sp[:-1]

    else:

        return ""

def printSites(args):
	
	if args[0] != "":
		
		print args[0]


nprocs = int(sys.argv[1])
filename = sys.argv[2]

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

fp = open(filename, "r")

src = Source(fp)
graph.add(src)

nd = FilterTagged(filterSites, 1)
graph.add(nd)

ser = Serializer(printSites, 1)
graph.add(ser)


src.add_edge(nd, 0)
nd.add_edge(ser, 0)


t0 = time.time()
sched.start()
t1 = time.time()

print "Execution time %.3f" %(t1-t0)
