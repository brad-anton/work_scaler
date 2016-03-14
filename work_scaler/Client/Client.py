"""Client.py
@brad_anton

Subscribes to a ZeroMQ message queue registers MAX_JOBS
workers. When a job is published from the server, a greenlet
is spawned and the do_work function is called. 

"""

import gevent
import gevent.monkey
gevent.monkey.patch_all()

from gevent.pool import Pool
from zmq import green as zmq
from uuid import uuid4

MAX_JOBS = 10 

class Client:
    def __init__(self, server='localhost', recv_port=5577, send_port=5578):
        """Each client is given a UUID and then 
        each greenlet is given a "worker" UUID
        to track the crawler/processing."""
        self.client_id = str(uuid4())

        # Tracks workers  
        self.jobs = []

        self.server = server 
        self.context = zmq.Context()

        # Channel for getting messages from the Server
        self.recv_p = recv_port 
        self.recv_s = self.context.socket(zmq.SUB)
        # Subscribe to only our messages
        self.recv_s.setsockopt(zmq.SUBSCRIBE, self.client_id)

        # Channel for sending messages to the Server
        self.send_p = send_port 
        self.send_s = self.context.socket(zmq.PUSH)

        # Setup Poller
        self.poller = zmq.Poller()
        self.poller.register(self.recv_s)

        self.running_greenlets = 0
        self.pool = Pool(MAX_JOBS)

    def recv(self, flags=0):
        client_id = self.recv_s.recv_string(flags=flags)
        msg = self.recv_s.recv_json(flags=flags)
        return msg
    
    def work_done(self, worker_id, result):
        print "[+] Worker {0} completed processing.".format(worker_id,result)
        # Remove job from our worker list
        for job in self.jobs:
            if job['worker'] == worker_id:
                self.jobs.remove(job)

        msg = {  'client': self.client_id,
                    'worker': worker_id, 
                    'type': 'job_complete', 
                    'result': result}
        # Let the server know we're done
        self.send_s.send_json(msg)

    def do_work(self, worker_id, work):
        """Override me and call work_done()"""
        result = []
        self.work_done(worker_id, result)

    def run(self):
        """Handles worker/job registration, and MQ polling
        """
        print "[+] Client {0} starting...".format(self.client_id)

        """Connect to Server's ZMQ. This is not the most secure
        way to use ZMQ, would be better to do a certificate exchange
        """
        self.send_s.connect("tcp://{0}:{1}".format(self.server, self.send_p))
        self.recv_s.connect("tcp://{0}:{1}".format(self.server, self.recv_p))

        while True:
            """Register as many workers as we're comfortable 
            with.
            """
            if len(self.jobs) < MAX_JOBS:
                # Workers are tracked by UUID
                worker_id = str(uuid4())

                print "[+] Registering {0}".format(worker_id)
                # Register the worker locally
                self.jobs.append( { 'worker': worker_id, 
                                    'work': None } )

                # Tell the server about our worer 
                registration = {'client': self.client_id,
                                'worker': worker_id, 
                                'type': 'register' }
                self.send_s.send_json(registration)

            """Poll the subscriber MQ, give it breaks to
            run other things"""
            s = dict(self.poller.poll(1))
            if (self.recv_s in s
                    and s[self.recv_s] == zmq.POLLIN):
                msg = self.recv()

                # Is the worker still around and unassigned? 
                for job in self.jobs:
                    if msg ['worker'] == job['worker'] and not job['work']:
                        job['work'] = msg['work']
                        self.pool.spawn(self.do_work, job['worker'], job['work'])
                        gevent.sleep(0)

if __name__ == '__main__':
    print '\n\n\n[!] This does nothing if called directly\n\n\n'
    c = Client()
    c.run()
