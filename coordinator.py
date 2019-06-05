# coding: utf-8
import csv
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
        # Queue of Reduce responses from worker
        # -----------

        self.reduce_responses = Queue()

        # -----------
        # Not used Variables but maybe usefull later
        # -----------

        self.ready_workers = []
        self.map_jobs = True

    def jobs_to_do(self, clientsocket):

        # If ready_workers > 0 start!
        padding = 0

        map_req = json.dumps(dict(task="map_request", blob=self.datastore_q.get()))
        size1 = len(map_req)
        print(str(size1) + map_req)
        clientsocket.sendall((str(size1).zfill(8) + map_req).encode("utf-8"))

        while True:

            bytes_size = clientsocket.recv(8).decode()
            xyz = int(bytes_size)
            new_msg = clientsocket.recv(xyz).decode("utf-8")

            if new_msg:
                msg = json.loads(new_msg)
                # Map replu e o datastore não esta vazio
                if msg["task"] == "map_reply" and not self.datastore_q.empty():
                    self.map_responses.put(msg["value"])
                    map_req = json.dumps(dict(task="map_request", blob=self.datastore_q.get()))
                    size = len(map_req)
                    clientsocket.sendall((str(size).zfill(8) + map_req).encode("utf-8"))

                # Map reply e o datastore está vazio
                if msg["task"] == "map_reply" and self.datastore_q.empty():
                    self.map_responses.put(msg["value"])
                    reduce_req = json.dumps(
                        dict(task="reduce_request", value=(self.map_responses.get(), self.map_responses.get())))
                    #print("map_reply and Empty = ", reduce_req)
                    size = len(reduce_req)
                    clientsocket.sendall((str(size).zfill(8) + reduce_req).encode("utf-8"))

                # Reduce Reply e já recebeu pelo menos um mapa
                if msg["task"] == "reduce_reply" and not self.map_responses.empty():
                    self.reduce_responses.put(msg["value"])
                    reduce_req = json.dumps(
                        dict(task="reduce_request", value=(self.map_responses.get(), self.map_responses.get())))
                    #print("Reduce Request = ", reduce_req)
                    size = len(reduce_req)
                    clientsocket.send((str(size).zfill(8) + reduce_req).encode("utf-8"))

                # Reduce Reply e já não há mapas
                if msg["task"] == "reduce_reply" and self.map_responses.empty():
                    self.reduce_responses.put(msg["value"])
                    if self.reduce_responses.qsize() > 1:
                        reduce_req = json.dumps(dict(task="reduce_request",
                                                     value=(self.reduce_responses.get(), self.reduce_responses.get())))
                        #print("Reduce Request = ", reduce_req)
                        size = len(reduce_req)
                        #print(str(size) + reduce_req)
                        clientsocket.sendall((str(size).zfill(8) + reduce_req).encode("utf-8"))
                    else:
                        print(list(self.reduce_responses.queue))

                if self.reduce_responses.qsize() == 0:
                    print("FINISHED")

                #print(new_msg)

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


        hist = []
        # store final histogram into a CSV file
        with args.out as f:
            csv_writer = csv.writer(f, delimiter=',',
            quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for w,c in hist:
                csv_writer.writerow([w,c])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r', encoding='UTF-8'), help='input file path')
    parser.add_argument('-o', dest='out', type=argparse.FileType('w', encoding='UTF-8'), help='output file path', default='output.csv')
    parser.add_argument('-b', dest='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()

    Coordinator().main(args)
