"""Feeder.py
@brad_anton

Reads data from a file and sends it via ZMQ

ROUGH DRAFT, THIS IS NOT COMPLETE!!!
"""

import gevent
import csv

from zmq import green as zmq
from collections import deque 
from random import randint
from pybloom import BloomFilter

SAMPLE_SIZE = 1000

class Feeder:
    def __init__(self, server='*', send_port=5576, db_file='authlogs-mar-11-ams-14-hours'):
        self.db_file = db_file

        self.context = zmq.Context()

        self.server = server
        self.send_p = send_port
        self.send_s = self.context.socket(zmq.PUSH)

        self.poller = zmq.Poller()

        self.work_q = deque()

    def build_bloom(self, alexa='top-1m.csv'):
        with open(alexa) as f:
            site_count = sum(1 for _ in f)
            bfilter = BloomFilter(site_count, 0.01)

            f.seek(0) # Reset after getting line count
            alexareader = csv.reader(f)
            for row in alexareader:
                bfilter.add(row[1])
            with open('alexa.bloom', 'wb') as out:
                bfilter.tofile(out)

    def in_alexa(self, host):
        with open('alexa.bloom') as file:
                bfilter = BloomFilter.fromfile(file)
                if host in bfilter:
                    return True
        return False


    def populate_queue(self):
        with open(self.db_file, 'rb') as f:
            total_entries = sum(1 for _ in f)
            start = randint(0, total_entries)

            # This could be faster, more elegant
            f.seek(0)
            for num, line in enumerate(f):
                if num == start:
                    break

            eof = False
            sample_count = 0
            while sample_count < SAMPLE_SIZE:
                try:
                    row = f.next().split('\t')
                    host = row[1][:-1]
                    host_2ld = row[6] 

                    if not self.in_alexa(host_2ld):
                        if host not in self.work_q: # We dedupe hosts but not 2ld 
                            self.work_q.appendleft(host)
                            sample_count += 1

                except StopIteration: # EOF
                    if eof:
                        print "[!] Got as many unique samples as I can find"
                        break
                    eof = True
                    f.seek(0)

        print self.work_q

    def start(self):
        pass

if __name__ == '__main__':
    f = Feeder()
    f.populate_queue()
        
        
