# THIS CODE IS CONTRIBUTED BY @MARYAM_YAZDI

import socket
import pickle
from tabulate import tabulate

HEADER = 10
PORT = 5030
SERVER = "192.168.1.106"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECT"

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

client.connect(ADDR)


class Student:
    def __int__(self, firstname, lastname, national_code, sh_code, credit_list):
        self.firstname = firstname
        self.lastname = lastname
        self.national_code = national_code
        self.sh_code = sh_code
        self.credit_list = credit_list


students_info = []
stuCount = int(input('Number of students: '))

for i in range(stuCount):
    print(f"Student {i + 1}")

    stu = Student()
    stu.__int__(input('Firstname: '), input('Lastname: '), input('National code:'),
                input('Birth code: '), [float(x) for x in input('List of 5 scores: ').split()])
    students_info.append(stu)


def send(msgg):
    message = pickle.dumps(msgg)
    msgg_length = len(message)
    send_length = str(msgg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    final_message = send_length + message
    client.send(final_message)


def display_response(title, data):
    table_headers = []

    if title == 'ave':
        table_headers = ["National Code", "Credits Average"]

    elif title == 'sor':
        table_headers = ["Birth Code", "Credits Average (asc sorted)"]

    elif title == 'max' or title == 'min':
        table_headers = ["First Name", "Last Name", "Average"]

    elif title == 'hel':
        print(data)

    elif title == 'ack':
        print(data)
        return

    print("\n\n", tabulate(data, headers=table_headers), "\n")


send(students_info)

# receive message
while True:

    response_title = client.recv(3).decode(FORMAT)
    if response_title:
        msg_length = client.recv(HEADER).decode(FORMAT)
        msg = client.recv(int(msg_length))
        response_data = pickle.loads(msg)

        if response_title == 'inv':
            print(response_data)
            command = input("\nWHAT DO YOU NEED TO DO?\n")
            send(command)
            continue

        else:
            display_response(response_title, response_data)

    command = input("\nWHAT DO YOU NEED TO DO?\n")
    send(command)
