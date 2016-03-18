
"""Server.py
@brad_anton

Uses a ZMQ publish/subscribe pattern to distribute 
work to workers and gets responses via a push/pull 
model. Work is instiated with the Server, but will
be eventually added to the queue via ZMQ. 

See __main__ for example

"""

import gevent

from datetime import datetime, timedelta 
from gevent.pool import Pool
from zmq import green as zmq
from uuid import uuid4
from collections import deque 

WORKER_TIMEOUT = 90 

class Server:
    def __init__(self, work=None, server='*', recv_port=5578, send_port=5577, feeder_server='localhost', feeder_port=5576):
        self.server_id = str(uuid4())
        
        self.server = server 
        self.context = zmq.Context()

        # Channel for getting messages from Clients 
        self.recv_p = recv_port 
        self.recv_s = self.context.socket(zmq.PULL)

        # Channel for sending messages to Clients 
        self.send_p = send_port 
        self.send_s = self.context.socket(zmq.PUB)

        # Setup Poller
        self.poller = zmq.Poller()
        self.poller.register(self.recv_s)
      
        self.workers = {}

        self.feeder_s = None

        if work:
            # Work is provided during instantiation
            self.work_q = deque(work)
        else:
            # Work from ZMQ Feeder
            print '[+] No work provided, preparing feeder queue'
            self.work_q = deque()
            self.feeder_server = feeder_server
            self.feeder_p = feeder_port
            self.feeder_s = self.context.socket(zmq.PULL)
            self.poller.register(self.feeder_s)

    def send(self, client_id, msg, flags=0):
        self.send_s.send_string(client_id, flags=flags|zmq.SNDMORE)
        self.send_s.send_json(msg, flags=flags)

    def process_result(self, msg):
        # Override me! 
        pass

    def process_feeder(self, msg):
        # Override me!
        pass

    def dispatcher(self):
        """Dispatches work to workers, cleans up inactive
        workers and clients
        """

        for client in self.workers.keys():
            # Clients without worker are removed
            if not self.workers[client]:
                self.workers.pop(client, None)
                continue 
            
            for worker in self.workers[client]:
                #Assign work to availabile workers
                if self.work_q and not worker['work']:
                    worker['work'] = self.work_q.pop()
                    print ("[+] Assigning {0} work:\n"
                            "\t{1}").format(worker['worker'], 
                                    worker['work'])
                    self.send(client, { 'worker': worker['worker'],
                                        'work': worker['work'] })

                """If a worker hasn't been seen for WORKER_TIMEOUT
                we remove them from our list and re-add the work
                back into the queue. 

                It's important to pick a realistic WORKER_TIMEOUT
                value or longer work will be forever in the queue.
                There is a possibility that we'll assign work 
                above to an inactive worker, and then it will 
                be cleaned up here. That's ok since the worker
                will complete on the client, and launch a new one
                which can be reassigned. 
                """
                if (datetime.now() > (worker['lastseen'] + 
                    timedelta(seconds=WORKER_TIMEOUT)) and
                    worker['work']):
                    print "[!] Removing inactive worker: {0}".format(
                        worker['worker'])
                    if worker['work']:
                        self.work_q.appendleft(worker['work'])
                    self.workers[client].remove(worker)


    def run(self):
        print "[+] Server {0} starting...".format(self.server_id)

        """Start Server's ZMQ channels. This is not the most secure
        way to use ZMQ, would be better to do a certificate exchange
        """
        self.send_s.bind("tcp://{0}:{1}".format(self.server, self.send_p))
        self.recv_s.bind("tcp://{0}:{1}".format(self.server, self.recv_p))
    
        if not self.work_q:
            # If empty at this stage in the game, its probably a feeder
            self.feeder_s.connect("tcp://{0}:{1}".format(self.feeder_server, self.feeder_p))

        while True:
            self.dispatcher()
            s = dict(self.poller.poll(100))
            if (self.recv_s in s
                    and s[self.recv_s] == zmq.POLLIN):
                msg = self.recv_s.recv_json()
                if 'type' in msg:
                    if msg['type'] == 'register':
                        if msg['client'] not in self.workers:
                            self.workers[msg['client']] = []
                        self.workers[msg['client']].append({ 'worker': msg['worker'],
                                                            'work': None,
                                                            'lastseen': datetime.now()})
                        print ("[+] Registered client: {0}"
                                "\n\tWorker: {1}").format(msg['client'],
                                                    msg['worker'])
                    elif msg['type'] == 'job_complete':
                        if msg['client'] in self.workers:
                            for worker in self.workers[msg['client']]:
                                if worker['worker'] == msg['worker']:
                                    print "[+] Worker {0} completed".format(worker['worker'])
                                    # Process the response
                                    self.process_result(msg)
                                    # Clean up the worker
                                    self.workers[msg['client']].remove(worker)
            elif (self.feeder_s in s
                    and s[self.feeder_s] == zmq.POLLIN):
                msg = self.feeder_s.recv_json()
                gevent.spawn(self.process_feeder, msg)

if __name__ == '__main__':
    """Work can be provided as individual 
     elements as show in urls below which is 
     meant to be consumed by a client like 
     CrawlerClient.py
     """
    urls = ['https://www.yahoo.com',
            'http://www.google.com',
            'http://www.gevent.org']

    """Or if the Client handles work as batches,
    like DnsClient.py it can be put in lists as 
    dns requests below
    """
    dns = [['https://www.yahoo.com',
            'http://www.google.com',
            'http://www.gevent.org']]
    s = Server(urls)
    #s = Server()
    s.run()
