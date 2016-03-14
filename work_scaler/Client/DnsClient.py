"""DnsClient.py
@brad_anton

Child class to resolves batches of URLs or hostnames.
Be sure to consider MAX_JOBS when providing batch size
as a very large batch size is multiplied by MAX_JOBs on 
any specific client.
"""

from Client import Client

import gevent
from gevent import socket

from tld import get_tld

class DnsClient(Client):
    def do_work(self, worker_id, work):
        """Assumes work is a list of hosts to resolve
        """
        jobs = [gevent.spawn(socket.gethostbyname, get_tld(host)) for host in work]
        gevent.joinall(jobs)
        result = [job.value for job in jobs]

        # Send the results
        self.work_done(worker_id, result)

if __name__ == '__main__':
    c = DnsClient()
    c.run()
