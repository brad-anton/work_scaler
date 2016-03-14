# work_scaler
A project for executing 'scalable' tasks using ZeroMQ and gevent.

## Install

First clone the project: 

```
git clone https://github.com/brad-anton/work_scaler.git
```

The set up a virutal environment

```
pip install virtualenv
cd work_scaler 
virtualenv venv
source venv/bin/activate
```

Install dependancies 

```
pip install -r requirements.txt
```
## Example

A really basic example is to run the server and the client:

### Running the Server
Start the Server with:

```
python work_scaler/Server/Server.py
```

### Running Clients
Run as many instances of the client on as many systems as you would like, but be sure you have enough work to keep them busy. 
```
python work_scaler/Client/CrawlerClient.py
```

This example comes with two Clients, `CrawlerClient.py` and `DnsClient.py`. If you'd like to test out `DnsClient.py`, modify `Server.py` to instantiate the Server with the `dns` variable. 
## Important Settings
### `WORKER_TIMEOUT`

The `WORKER_TIMEOUT` in `Server.py` defines how long the server should wait before considering a client/worker no longer available to take work. 

### `MAX_JOBS`

The `MAX_JOBS` in `Client.py` defines how many jobs a single client should take. Note this can be tricky for Clients that do batch processing such as `DnsClient.py`. 



