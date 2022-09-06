import socket
import pickle
import threading
import psycopg2

HEADER = 10
TITLE = 3
PORT = 7800
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECT"
INVALID_INPUT = "INVALID COMMAND. ENTER 'Help' TO GET THE LIST OF COMMANDS."

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)


class Student:
    def __int__(self, firstname, lastname, national_code, credit_list):
        self.firstname = firstname
        self.lastname = lastname
        self.national_code = national_code
        self.credit_list = credit_list


class COLORS:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    FAIL = '\033[91m'
    PURPLE = '\033[35m'
    LIGHTBLUE = '\033[94m'
    RESET = '\033[0m'


def create_table():
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    postgres_command = "CREATE TABLE students " \
                       "(id BIGSERIAL NOT NULL PRIMARY KEY," \
                       "faculty INT," \
                       "firstname VARCHAR(250)," \
                       "lastname VARCHAR(250)," \
                       "nationalCode VARCHAR(100)," \
                       "credits FLOAT[] );"
    cursor.execute(postgres_command)
    db_conn.commit()
    db_conn.close()


def insert(obj, faculty_no):
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    postgres_command = "INSERT INTO students " \
                       "(Faculty, FirstName, LastName, Nationalcode, credits[0], " \
                       "credits[1], credits[2], credits[3], credits[4]) " \
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    params = (faculty_no, obj.firstname, obj.lastname, obj.national_code, obj.credit_list[0]
              , obj.credit_list[1], obj.credit_list[2], obj.credit_list[3], obj.credit_list[4],)
    cursor.execute(postgres_command, params)
    db_conn.commit()
    db_conn.close()


def average(faculty_no):
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    postgres_command = "SELECT nationalcode,(credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5 " \
                       "FROM students where Faculty= %s;"
    params = (faculty_no,)
    cursor.execute(postgres_command, params)
    sql_result = cursor.fetchall()
    ave_list = []
    for row in sql_result:
        ave_list.append((row[0], row[1]))
    db_conn.close()
    return ave_list


def sort(faculty_no):
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    sqlcommand = "SELECT lastname,nationalcode,(credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5 " \
                 "FROM students where Faculty= %s order by 3 asc ;"
    params = (faculty_no,)
    cursor.execute(sqlcommand, params)
    sql_result = cursor.fetchall()
    sorted_ave_list = []
    for row in sql_result:
        sorted_ave_list.append((row[0], row[1], row[2]))
    db_conn.close()
    return sorted_ave_list


def max_ave(faculty_no):
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    sqlcommand = "SELECT firstname,lastname,(credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5 FROM students" \
                 " WHERE (credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5 = " \
                 "(SELECT MAX((credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5) FROM students WHERE Faculty=%s)" \
                 " AND Faculty=%s;"
    params = (faculty_no, faculty_no)
    cursor.execute(sqlcommand, params)
    sql_result = cursor.fetchall()
    max_ave_list = []
    for row in sql_result:
        max_ave_list.append((row[0], row[1], row[2]))
    return max_ave_list


def min_ave(faculty_no):
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    sqlcommand = "SELECT firstname,lastname,(credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5 FROM students" \
                 " WHERE (credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5 = " \
                 "(SELECT MIN((credits[0]+credits[1]+credits[2]+credits[3]+credits[4])/5) FROM students " \
                 "WHERE Faculty=%s)" \
                 " AND Faculty=%s;"
    params = (faculty_no, faculty_no)
    cursor.execute(sqlcommand, params)
    sql_result = cursor.fetchall()
    min_ave_list = []
    for row in sql_result:
        min_ave_list.append((row[0], row[1], row[2]))
    return min_ave_list


def return_db():
    db_conn = psycopg2.connect(database="db1", user='postgres', password='M123456m@', host='127.0.0.1',
                               port='5432')
    cursor = db_conn.cursor()
    sqlcommand = "SELECT faculty,firstname,lastname,nationalcode,credits[0]," \
                 "credits[1],credits[2],credits[3],credits[4] FROM students"
    cursor.execute(sqlcommand)
    sql_result = cursor.fetchall()
    db = []
    for row in sql_result:
        for j in row:
            print("j",j)
            db.append(j)
    return db

faculty_id = 0


def handle_client(conn, addr, faculty):
    print(f"{COLORS.GREEN}[NEW CONNECTION]{COLORS.RESET} {addr} connected.")

    stu_count = 0
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg = conn.recv(int(msg_length))
            received_data = pickle.loads(msg)
            print(f"{COLORS.PURPLE}[REQUEST] {COLORS.RESET}", received_data)

            if received_data == DISCONNECT_MESSAGE:
                connected = False

            elif received_data == "Average":
                send(average(faculty), 'ave', conn)

            elif received_data == "Sort":
                send(sort(faculty), 'sor', conn)

            elif received_data == "Max":
                send(max_ave(faculty), 'max', conn)

            elif received_data == "Min":
                send(min_ave(faculty), 'min', conn)

            elif received_data == "Show":
                send(return_db(), 'sho', conn)
            else:
                if stu_count != 0:
                    send(INVALID_INPUT, 'inv', conn)
                else:
                    for obj in received_data:
                        insert(obj, faculty)
                        stu_count += 1
                    print(f"{COLORS.GREEN}[INSERT DATABASE]{COLORS.RESET} {stu_count} new records inserted.")
                    send(f"ALL {stu_count} RECORDS INSERTED TO DATABASE SUCCESSFULLY.", 'ack', conn)

    conn.close()


def start():
    global faculty_id
    server.listen()
    print(f"{COLORS.GREEN}[LISTENING]{COLORS.RESET} server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        faculty_id += 1
        thread = threading.Thread(target=handle_client, args=(conn, addr, faculty_id))
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
    title_header = msg_title[0:TITLE].encode(FORMAT)
    data = pickle.dumps(msg)
    length_header = str(len(data)).encode(FORMAT)
    length_header += b' ' * (HEADER - len(length_header))
    send_message = title_header + length_header + data
    print(f"{COLORS.LIGHTBLUE}[RESPONSE SENT]{COLORS.RESET}", send_message)
    conn.send(send_message)


print(f"{COLORS.GREEN}[STARTING]{COLORS.RESET} server is starting...")
client_count = int(input('Enter the number of faculties:'))
start()
