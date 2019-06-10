# coding: utf-8

import logging
import argparse
import os
import socket
import string
import json


logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

class Worker(object):
    def __init__(self):

        # -----------
        # Set Worker pid, port and status
        # -----------

        self.id = os.getpid()
        self.port = 8081
        self.worker_status = "READY"

        # -----------
        # Worker Socket
        # -----------

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # -----------
        # Worker register msg with pid
        # -----------

        self.register_msg = json.dumps({
            "task": "register",
            "id": self.id
        })

        # -----------
        # Bytes padding for msgs
        # -----------

        self.padding = 0

    def handle_map_request(self, blob):

        lista = []
        punct = list(string.punctuation)

        frase = blob.split()

        lista_f = []
        for palavra in frase:
            palavra = ''.join(i for i in palavra if i not in punct and not i.isdigit())
            for c in punct:
                palavra = palavra.strip(c)
            lista_f.append(palavra)

            if palavra == '':
                lista_f.remove(palavra)
        for w in lista_f:
            lista.append((w, 1))
        # print()
        # print(lista)
        # print("------------------------------------------------------")

        return json.dumps(dict(task="map_reply", value=lista))

    def handle_reduce_request(self, value):

        reduced_list = []
        words = []
        for nval in value:
            for w, nr in nval:
                w = w.lower()
                if w not in words:
                    words.append(w)
                    reduced_list.append((w, nr))
                else:
                    for i in reduced_list:
                        if i[0] == w:
                            reduced_list.remove((w, i[1]))
                            nr = nr + i[1]
                    reduced_list.append((w.lower(), nr))
        # print()
        # print(reduced_list)
        # print("------------------------------------------------------------")
        return json.dumps(dict(task="reduce_reply", value=reduced_list))

    def main(self, args):
        logger.debug('Connecting to %s:%d', args.hostname, args.port)

        try:
            self.sock.connect((args.hostname, args.port))
            self.sock.sendall(self.register_msg.encode("utf-8"))

            while True:

                bytes_size = self.sock.recv(8).decode()
                xyz = int(bytes_size)
                json_msg = self.sock.recv(xyz).decode("utf-8")

                if json_msg:
                    msg = json.loads(json_msg)
                    if msg["task"] == "map_request":
                        map_reply = self.handle_map_request(msg["blob"])
                        logger.debug('Handle Map Request')
                        print(map_reply)
                        size = len(map_reply)
                        self.sock.sendall((str(size).zfill(8) + map_reply).encode("utf-8"))
                        logger.debug('Send Map Reply')
                    if msg["task"] == "reduce_request":
                        reduce_reply = self.handle_reduce_request(msg["value"])
                        logger.debug('Handle Reduce Request')
                        print(reduce_reply)
                        size = len(reduce_reply)
                        self.sock.sendall((str(size).zfill(8) + reduce_reply).encode("utf-8"))
                        logger.debug('Send Reduce Reply')
                    if msg["task"] == "shutdown":
                        print("JOB COMPLETED WITH SUCCESS! >> SHUTDOWN")
                        break

        except socket.error:
            print("Error to connect with Coordinator")
        finally:
            self.sock.close()


def tokenizer(txt):
    tokens = txt.lower()
    tokens = tokens.translate(str.maketrans('', '', string.digits))
    tokens = tokens.translate(str.maketrans('', '', string.punctuation))
    tokens = tokens.translate(str.maketrans('', '', '«»'))
    tokens = tokens.rstrip()
    return tokens.split()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--id', dest='id', type=int, help='worker id', default=0)
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()

    Worker().main(args)
