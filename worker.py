# coding: utf-8
import json
import logging
import argparse
import os
import socket
from threading import Thread

# socket.listen(backlog) Listen for connections made to the socket.
# The backlog argument specifies the maximum number of queued connections and should be at least 1; the maximum value is system-dependent (usually 5).

#

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

class Worker(object):
    def __init__(self):
        self.id = os.getpid()
        self.port = 8081
        self.worker_status = "READY"

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.register_msg = json.dumps({
            "task": "register",
            "id": self.id
        })

    def jobs_to_do(self):

        workflow_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workflow_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        workflow_sock.bind((args.hostname, args.port))

        workflow_sock.listen(5)

        while True:
            conn = workflow_sock.accept()
            json_msg = conn[0].recv().decode("utf-8")

            if json_msg:
                msg = json.loads(json_msg)
                if msg["task"] == "map_request":
                    print("MAP")
                    # TODO: HANDLE MAP REQUEST
                if msg["task"] == "reduce_request":
                    print("REDUCE")
                    # TODO: HANDLE REDUCE REQUEST

    def main(self, args):
        logger.debug('Connecting to %s:%d', args.hostname, args.port)

        try:
            self.sock.connect((args.hostname, args.port))

            # process_messages = Thread(target=self.jobs_to_do, args=())
            # process_messages.start()

            self.sock.sendall(self.register_msg.encode("utf-8"))

            while True:

                # self.sock.recv(1024)
                json_msg = self.sock.recv(5000).decode("utf-8")


                msg = json.loads(json_msg)
                if msg["task"] == "register":
                    print("REGISTER DETECTED")
                if msg["task"] == "map_request":
                    print(msg)
                    # TODO: HANDLE MAP REQUEST
                if msg["task"] == "reduce_request":
                    print("REDUCE")
                    # TODO: HANDLE REDUCE REQUEST

        except socket.error:
            print("Error to connect with Coordinator")
        finally:
            self.sock.close()




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    Worker().main(args)

