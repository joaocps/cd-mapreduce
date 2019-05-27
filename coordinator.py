# coding: utf-8

import json
import logging
import argparse
import socket
import threading

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')


class Coordinator(object):

    def __init__(self):

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger = logging.getLogger('Coordinator')

        self.ready_workers = []


    def main(self, args):

        datastore = []
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
                datastore.append(blob)

        # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.socket.bind(("localhost", args.port))
        # self.socket.listen(5)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", args.port))
        sock.listen(5)

        clientsocket, address = sock.accept()

        message_chunks = []
        while True:
            data = clientsocket.recv(1024).decode("utf-8")
            if data:
                msg = json.loads(data)
                if msg["task"] == "register":
                    print("NEW WORKER HERE")
                    print("Worker id: " + str(msg["id"]))
                    # TODO
                if msg["task"] == "map_reply":
                    print("NEW MAP_REPLY HERE")
                    # TODO
                if msg["task"] == "reduce_reply":
                    print("NEW REDUCE_REPLY HERE")
                    # TODO
            if not data:
                break
            message_chunks.append(data)

        clientsocket.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r'), help='file path')
    parser.add_argument('-b', dest='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()

    Coordinator().main(args)
