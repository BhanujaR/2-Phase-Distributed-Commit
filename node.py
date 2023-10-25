import logging
import socket
import time

TC_HOST = 'localhost' 
TC_PORT = 8065      

def send_message(connection, message):
    print('node: Sending', message)
    connection.sendall(message.encode())

def receive_message(connection):
    data = connection.recv(1024).decode()
    print('node: Received', data)
    return data

def ask_decision(message, option_yes, option_no):
    while True:
        answer = input(f'{message} ({option_yes}/{option_no}) ')
        if answer == option_yes:
            return True
        if answer == option_no:
            return False

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
    node_socket.connect((TC_HOST, TC_PORT))
    node_socket.settimeout(20)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - [ %(message)s ]",
        datefmt='%d-%b-%y %H:%M:%S',
        force=True,
        handlers=[logging.FileHandler("node_out.log"), logging.StreamHandler()]
    )

    while True:
        try:
            data_received = receive_message(node_socket)

            if data_received.startswith('Prepare'):
                print('node: Transaction Start')

                if ask_decision('node: do you want to commit? yes:commit or no:abort:',
                                'yes', 'no'):
                    logging.info('node: Ready to commit')
                    send_message(node_socket, 'commit')
                else:
                    logging.info('node: Aborting')
                    send_message(node_socket, 'abort')

            print('node: Ready for next step')
            data_received = receive_message(node_socket)

            if data_received == 'all-abort':
                logging.info('node: Transaction ABORTED')
                print('node: Transaction ABORTED')
            elif data_received == 'all-commit':
                logging.info('node: Transaction COMMITTED')
                print('node: Transaction COMMITTED')
            send_message(node_socket, 'ACK')
        except socket.timeout:
            print('node: Timeout waiting for coordinator response')
        except KeyboardInterrupt:
            print('node: Ending connection')
            break
