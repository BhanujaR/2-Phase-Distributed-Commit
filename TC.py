import socket
import threading
import logging
from multiprocessing import Lock
import time

HOST = 'localhost'
PORT = 8065

class Coordinator:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.clients = []
        self.started = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_thread = threading.Thread(target=self.accept_connections)
        self.counter = 0
        self.counter_message = 'Prepare'
        self.socket.bind((self.host, self.port))
        self.socket.listen()

    def accept_connections(self):
        while self.started:
            connection, _ = self.socket.accept()
            self.clients.append(connection)

    def start(self):
        self.started = True
        self.listen_thread.start()

    def stop(self):
        self.started = False
        for client_connection in self.clients:
            client_connection.close()
        self.listen_thread.join()
        self.socket.close()

    def send_to_clients(self, message):
        
        print('TC: Sending message to nodes:', message)
        for client_connection in self.clients:
            if message == self.counter_message:
                if self.counter == 2:
                    logging.info('TC: Aborting transaction')
                    self.send_to_clients('ALL-ABORT')
                    print('TC: Transaction ABORTED')
                    return

            self.counter += 1
            client_connection.sendall(message.encode())


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - [ %(message)s ]",
        datefmt='%d-%b-%y %H:%M:%S',
        force=True,
        handlers=[logging.FileHandler("TC_out.log"), logging.StreamHandler()]
    )

    TC_server = Coordinator(HOST, PORT)
    TC_server.start()
    try:
        while True:
            transaction_query = input('Node: enter the transaction: ')
            print('node: Transaction started')
            logging.info('TC: Initiating transaction')
            TC_server.send_to_clients(f'Prepare:{transaction_query}')
            
            vote_results = set()
            for client_conn in TC_server.clients:
                vote = client_conn.recv(1024)
                if vote:
                    print('node: received vote from node:', vote.decode())
                    vote_results.add(vote.decode())
                else:
                    TC_server.clients.remove(client_conn)

            if 'abort' in vote_results:
                logging.info('TC: Aborting transaction')
                print('TC: Waiting for nodes to ACKNOWLEDGE ABORT')
                TC_server.send_to_clients('ALL-ABORT')
                print('TC: Transaction ABORTED')
            else:
                logging.info('TC: Committing transaction')
                print('TC: Waiting for nodes to ACKNOWLEDGE COMMIT')
                TC_server.send_to_clients('ALL-COMMIT')
                print('TC: Transaction COMMITTED')

            fresh_vote_results = set()
            for client_conn in TC_server.clients:
                vote = client_conn.recv(1024)
                if vote:
                    print('node: Received ACKNOWLEDGEMENT from node:', vote.decode())
                    fresh_vote_results.add(vote.decode())
                else:
                    TC_server.clients.remove(client_conn)
            print('TC: ACKNOWLEDGEMENTS received from nodes:', fresh_vote_results)
            logging.info('TC: Transaction ended')
    except KeyboardInterrupt:
        TC_server.stop()
