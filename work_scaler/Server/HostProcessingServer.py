"""HostProcessingServer.py
@brad_anton

"""
from Server import Server
import csv
from pybloom import BloomFilter
from tld import get_tld

class HostProcessingServer(Server):
    def process_result(self, msg):
        # Override me! 
        pass

    def process_feeder(self, msg):
        # Override me!
        if 'work' in msg:
            for work in msg['work']:
                pass

if __name__ == '__main__':
    s = HostProcessingServer()
    #s.build_bloom()
