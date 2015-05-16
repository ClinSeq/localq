import Pyro4
from LocalQ import LocalQServer

__author__ = 'dankle'




import argparse
import os

ap = argparse.ArgumentParser()
ap.add_argument('-n', help="Number of available cores", action="store", required=False, default=1)
ap.add_argument('-i', help="Time interval (in seconds) to check queue and start jobs", action="store", required=False, default=30)

opts = ap.parse_args()

daemon = Pyro4.Daemon()
localqserver = LocalQServer.LocalQServer(opts.n, opts.i)
uri = daemon.register(localqserver)

ns = Pyro4.locateNS()
ns.register("localqd", uri)

localqserver.run()

daemon.requestLoop()

