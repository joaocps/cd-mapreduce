# coding: utf-8
import json
import logging
import argparse
import os
import socket

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

class Worker(object):
    def __init__(self):
        self.id = os.getpid()

        self.register_msg = json.dumps({
            "task": "register",
            "id": self.id
        })

    def register_to_coordinator(self, args):

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((args.hostname, args.port))
            sock.sendall(self.register_msg.encode("utf-8"))
        except socket.error:
            print("Error to connect with Coordinator")
        finally:
            sock.close()

    def main(self, args):
        logger.debug('Connecting to %s:%d', args.hostname, args.port)

        self.register_to_coordinator(args)




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    Worker().main(args)

