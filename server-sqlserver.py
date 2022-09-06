import pickle
import socket
import threading
import pyodbc
import os

HEADER = 10
PORT = 5020
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECT"
CONNECTION_STRING = 'Driver={SQL Server};' \
                    'Server=DESKTOP-1BU304E;' \
                    'Database=Students;' \
                    'Trusted_Connection=yes;'
INVALID_INPUT = "INVALID COMMAND. ENTER 'Help' TO GET THE LIST OF COMMANDS."

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)


class Student:
    def __int__(self, firstname, lastname, national_code, sh_code, credit_list):
        self.firstname = firstname
        self.lastname = lastname
        self.national_code = national_code
        self.sh_code = sh_code
        self.credit_list = credit_list


class COLORS:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    FAIL = '\033[91m'
    PURPLE = '\033[35m'
    LIGHTBLUE = '\033[94m'
    RESET = '\033[0m'


def display_help_text():
    help_text = "'Average': RETURNS A (NATIONAL C0DE , AVERAGE) LIST OF YOUR FACULTY STUDENTS :::\n" \
                "'Sort': RETURNS AN ASCENDING SORTED LIST OF (BIRTH C0DE , AVERAGE) OF YOUR FACULTY STUDENTS :::\n" \
                "'Max': RETURNS THE FIRST NAME, LAST NAME AND AVERAGE OF MAX-AVERAGE STUDENT IN YOUR FACULTY :::\n" \
                "'Min': RETURNS THE FIRST NAME, LAST NAME AND AVERAGE OF MIN-AVERAGE STUDENT IN YOUR FACULTY :::\n"
    return help_text


def insert(obj, faculty_no):
    db_conn = pyodbc.connect(CONNECTION_STRING)
    cursor = db_conn.cursor()
    sqlcommand = "INSERT INTO table1 " \
                 "(Faculty, FirstName, LastName, Ncode, Shcode, credit1, credit2, credit3, credit4, credit5) " \
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    params = [(faculty_no, obj.firstname, obj.lastname, obj.national_code, obj.sh_code,
               obj.credit_list[0], obj.credit_list[1],
               obj.credit_list[2], obj.credit_list[3], obj.credit_list[4])]
    cursor.executemany(sqlcommand, params)
    db_conn.commit()
    db_conn.close()


def average(faculty_no):
    db_conn = pyodbc.connect(CONNECTION_STRING)
    cursor = db_conn.cursor()
    sqlcommand = "SELECT Ncode,(credit1+credit2+credit3+credit4+credit5)/5 " \
                 "FROM table1 where Faculty=?"
    cursor.execute(sqlcommand, faculty_no)
    sql_result = cursor.fetchall()
    ave_list = []
    for row in sql_result:
        ave_list.append((row[0], row[1]))
    db_conn.close()
    return ave_list


def sort(faculty_no):
    db_conn = pyodbc.connect(CONNECTION_STRING)
    cursor = db_conn.cursor()
    sqlcommand = "SELECT Shcode,(credit1+credit2+credit3+credit4+credit5)/5 " \
                 "FROM table1 where Faculty=? order by 2 asc"
    cursor.execute(sqlcommand, faculty_no)
    sql_result = cursor.fetchall()
    sorted_ave_list = []
    for row in sql_result:
        sorted_ave_list.append((row[0], row[1]))
    db_conn.close()
    return sorted_ave_list


def max_ave(faculty_no):
    db_conn = pyodbc.connect(CONNECTION_STRING)
    cursor = db_conn.cursor()
    sqlcommand = "SELECT FirstName,LastName,(credit1+credit2+credit3+credit4+credit5)/5 FROM table1" \
                 " WHERE (credit1+credit2+credit3+credit4+credit5)/5 = " \
                 "(SELECT MAX((credit1+credit2+credit3+credit4+credit5)/5) FROM table1 WHERE Faculty=?)" \
                 " AND Faculty=?"
    params = [faculty_no, faculty_no]
    cursor.execute(sqlcommand, params)
    sql_result = cursor.fetchall()
    max_ave_list = []
    for row in sql_result:
        max_ave_list.append((row[0], row[1], row[2]))
    return max_ave_list


def min_ave(faculty_no):
    db_conn = pyodbc.connect(CONNECTION_STRING)
    cursor = db_conn.cursor()
    sqlcommand = "SELECT FirstName, LastName,(credit1+credit2+credit3+credit4+credit5)/5 FROM table1" \
                 " WHERE (credit1+credit2+credit3+credit4+credit5)/5 = " \
                 "(SELECT MIN((credit1+credit2+credit3+credit4+credit5)/5) FROM table1 WHERE Faculty=?)" \
                 " AND Faculty=?"
    params = [faculty_no, faculty_no]
    cursor.execute(sqlcommand, params)
    sql_result = cursor.fetchall()
    min_ave_list = []
    for row in sql_result:
        min_ave_list.append((row[0], row[1], row[2]))
    return min_ave_list


faculty_id = 0


def handle_client(conn, addr):
    print(f"{COLORS.GREEN}[NEW CONNECTION]{COLORS.RESET} {addr} connected.")

    global faculty_id
    faculty_id += 1
    stu_count = 0

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        print(msg_length)
        if msg_length:
            msg = conn.recv(int(msg_length))
            received_data = pickle.loads(msg)
            print(f"{COLORS.PURPLE}[REQUEST] {COLORS.RESET}", received_data)

            if received_data == DISCONNECT_MESSAGE:
                connected = False

            elif received_data == "Average":
                send(average(faculty_id), 'ave', conn)

            elif received_data == "Sort":
                send(sort(faculty_id), 'sor', conn)

            elif received_data == "Max":
                send(max_ave(faculty_id), 'max', conn)

            elif received_data == "Min":
                send(min_ave(faculty_id), 'min', conn)

            elif received_data == "Help":
                send(display_help_text(),'hel',conn)
            else:
                if stu_count != 0:
                    send(INVALID_INPUT, 'inv', conn)
                else:
                    for obj in received_data:
                        insert(obj, faculty_id)
                        stu_count += 1
                    print(f"{COLORS.GREEN}[INSERT DATABASE]{COLORS.RESET} {stu_count} new records inserted.")
                    send(f"ALL {stu_count} RECORDS INSERTED TO DATABASE SUCCESSFULLY.", 'ack', conn)

    conn.close()


def start():
    server.listen(client_count)
    print(f"{COLORS.GREEN}[LISTENING]{COLORS.RESET} server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"\n{COLORS.GREEN}[ACTIVE CONNECTIONS]{COLORS.RESET} {threading.activeCount() - 1}")


def send(msg, msg_title, conn):
    """
        Protocol:
        Unlike the protocol used for sending message from clients to server;
        The first 3 characters of each sending message (from server to client)
        determine message (response) title. Used by the aim of choosing proper table headers
        for print the response for client.

        The next 10 characters of the message (from server to client) is the header as well.
        Which determines the length of response.
    """
    title_header = msg_title[0:3].encode(FORMAT)
    data = pickle.dumps(msg)
    length_header = str(len(data)).encode(FORMAT)
    length_header += b' ' * (HEADER - len(length_header))
    send_message = title_header + length_header + data
    print(f"{COLORS.LIGHTBLUE}[RESPONSE SENT]{COLORS.RESET}", send_message)
    conn.send(send_message)


print(f"{COLORS.GREEN}[STARTING]{COLORS.RESET} server is starting...")
client_count = int(input('Enter the number of faculties:'))
start()
