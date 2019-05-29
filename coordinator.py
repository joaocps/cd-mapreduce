# coding: utf-8

import json
import logging
import argparse
import socket
import threading
from queue import Queue

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')


class Coordinator(object):

    def __init__(self):

        # -----------
        # Coordinator Socket
        # -----------

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger = logging.getLogger('Coordinator')

        # -----------
        # Datastore of initial blobs
        # -----------

        self.datastore = []
        self.datastore_q = Queue()

        # -----------
        # Queue of Map responses from worker
        # -----------

        self.map_responses = Queue()

        # -----------
        # Not used Variables but maybe usefull later
        # -----------

        self.ready_workers = []
        self.work = True

    def jobs_to_do(self, clientsocket):

        # If ready_workers > 0 start!

        map_req = json.dumps(dict(task="map_request", blob=self.datastore_q.get()))
        clientsocket.sendall(map_req.encode("utf-8"))

        while True:
            # clientsocket.settimeout(5)
            new_msg = clientsocket.recv(8192).decode("utf-8")

            if new_msg:
                msg = json.loads(new_msg)
                if msg["task"] == "map_reply":
                    self.map_responses.put(msg["value"])
                    map_req = json.dumps(dict(task="map_request", blob=self.datastore_q.get()))
                    clientsocket.sendall(map_req.encode("utf-8"))
                    if self.datastore_q.empty():
                        reduce_req = json.dumps(dict(task="reduce_request", value=self.map_responses.get()))
                        clientsocket.sendall(reduce_req.encode("utf-8"))
                        if msg["task"] == "reduce_reply":
                            print("IM HERE")


            # if self.datastore_q.empty():
            #     print(list(self.map_responses.queue))
        # clientsocket.sendall(map_req.encode("utf-8"))

    def main(self, args):

        with args.file as f:
            while True:
                blob = f.read(args.blob_size)
                if not blob:
                    break
                # This loop is used to not break word in half
                while not str.isspace(blob[-1]):
                    ch = f.read(1)
                    if not ch:
                        break
                    blob += ch
                logger.debug('Blob: %s', blob)
                self.datastore.append(blob)
                self.datastore_q.put(blob)

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("localhost", args.port))
        self.socket.listen(5)

        clientsocket, address = self.socket.accept()

        json_msg = clientsocket.recv(1024).decode("utf-8")

        if json_msg:
            msg = json.loads(json_msg)
            if msg["task"] == "register":
                process_messages = threading.Thread(target=self.jobs_to_do, args=(clientsocket,))
                process_messages.start()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r'), help='file path')
    parser.add_argument('-b', dest='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()

    Coordinator().main(args)
