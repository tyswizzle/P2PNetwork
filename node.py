import socket
import sys
import time
import threading
import uuid
import os
from os import listdir
from os.path import isfile, join
import file
import files
import PySimpleGUI as sg
import shutil
import errno
import logo
import base64

logo = base64.b64decode(logo.logo)

HOST = '192.168.2.151'
PORT = 8000
BUFFER_SIZE = 1024
ENCODING = 'utf-8'

sock_list = []
masterlist = []

STX = "T02"
ETX = "T03"
EOT = "T04"
DEL = "DEL"  # send del + file name
ADD = "ADD"  # send add + file name + filesize + file data
UPD = "UPD"  # send udp + file name + filesize + file data (overwrite the file with this name)
MAS = "MAS"  # send MAS + masterlist
DIS = "DIS" # send disconnect command
CCLEN = len(DEL)


class Node():

    def __init__(self, name, myport, host, port, filepath=os.getcwd()):
        self.myhost = self.get_my_ip()
        self.myport = myport  # Only used if you are starting a host node
        self.host = host    # Host address that you are connecting to
        self.port = port    # Host port that you are connecting to
        self.name = name
        self.fp = filepath + "/share/" # Standard filepath for transferring for all nodes

        # Set flag for when a node is closed
        self.nodeOpen = True

        print("Node created!\nAddress: %s:%d\nUsername: %s" % (self.host, self.port, self.name))

    '''
    Get all of the current files in the share folder ** Will be empty if you are joining as a client
    '''
    def get_file_list(self):
        return [f for f in listdir(self.fp) if isfile(join(self.fp, f))]

    '''
    Only used for the host node. Gets its local ip address by pining a website. Used to let other nodes what address
    to connect to
    '''
    def get_my_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("www.google.com", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip


'''
Create a host node for other clients to connect to. Handles bidirectional communication between the host node
and all of the client nodes. Serves as a mediary between all nodes.
'''
def host_node(name, n):
    s = socket.socket()
    host = n.get_my_ip()
    s.bind((host, n.port))
    '''
    5 chosen for testing purposes. This program is not meant to scale due to the nature
    of the program being utilized on local machines
    '''
    s.listen(5)

    print("Started Server at: (%s:%d)" % (host, n.port))

    # Start listening for connections
    t = threading.Thread(target=host_accept, args=(s, n))
    t.start()

    # Start the GUI for the user to use
    host_console(name, n)


'''
Called when the host activates the Add button on their GUI. Sends the given file to all of the client nodes
'''
def host_add_file(path, name, size=None):
    global masterlist
    global sock_list

    head, tail = os.path.split(path)
    masterlist.append(file.File(name))
    try:
        shutil.move(path, os.path.join(files.get_working_directory(), tail))  # adds it to share folder
    except Exception as e:
        print("Error in host_add_file: %s" % str(e))

    # Sends the file to all client nodes
    for sock in sock_list:
        truepath = os.path.join(files.get_working_directory(), name) # Full path of the file including name and filetype
        print(truepath)
        if size:
            fsize = size
        else:
            fsize = os.path.getsize(truepath)
        message = ADD + name + ETX + str(fsize) + EOT
        print("Sending: %s to %s" % (message, str(sock.getpeername())))
        sock.send(message.encode(ENCODING))
        with open(truepath, 'rb') as k:
            bytessent = 0
            while bytessent < fsize:
                data = k.read(BUFFER_SIZE)
                sock.send(data)
                bytessent += len(data)

            k.close()
            print("Sent Successfully!")


'''
Called when the host presses the Delete button in the GUI. Deletes the file from the local system
and sends out a command for the other nodes to delete that file as well.
'''
def host_delete_file(filename):

    try:
        os.remove(files.get_filepath(filename))  # removes it locally
        for file in masterlist:
            if file.name == filename:
                masterlist.remove(file) # Add to the master list of all files that are supposed to be synchronized
        print("File removed")
    except Exception as e:
        print("Error in host_delete_file: %s" % str(e))

    # Send out command to all current connections to delete that file as well
    for sock in sock_list:
        message = DEL + filename + EOT
        print("Sending: %s to %s" % (message, str(sock.getpeername())))
        sock.send(message.encode(ENCODING))
        time.sleep(0.01)
        print("Sent Successfully!")

'''
Called when the host machine activates the Update button on their GUI. Sends out the updated file
to overwrite the client nodes' file of the same name with the update file
'''
def host_update_file(name):
    global masterlist
    global sock_list

    myfiles = files.scan() # Get the list of the current files in our share folder
    for item in myfiles:
        if item.name == name: # find the path of the file that we are updating
            path = item.path

    # Send command to all client nodes to overwrite the file with its updated version
    for sock in sock_list:
        truepath = path
        fsize = os.path.getsize(truepath)
        message = UPD + name + ETX + str(fsize) + EOT
        print("Sending: %s to %s" % (message, str(sock.getpeername())))
        sock.send(message.encode(ENCODING))
        time.sleep(0.01)
        with open(truepath, 'rb') as f:
            bytessent = 0
            while bytessent < fsize:
                data = f.read(BUFFER_SIZE)
                sock.send(data)
                bytessent += len(data)

            f.close()
            print("Sent Successfully!")


'''
Sends a specified file to all of the client nodes. Called whenever a file needs to be sent.
'''
def host_send_file(path, name):
    global masterlist
    global sock_list

    # Send the file to all client nodes.
    for sock in sock_list:
        truepath = files.get_working_directory() + "/" + name
        fsize = os.path.getsize(truepath)
        message = ADD + name + ETX + str(fsize) + EOT
        print("Sending: %s to %s" % (message, str(sock.getpeername())))
        sock.send(message.encode(ENCODING))
        with open(truepath, 'rb') as k:
            bytessent = 0
            while bytessent < fsize:
                data = k.read(BUFFER_SIZE)
                sock.send(data)
                bytessent += len(data)

            k.close()
            print("Sent Successfully!")


'''
Called when the host receives a command to add a file to their directory. Then sends that file to 
all other client nodes
'''
def host_add_file_from_client(sock, n, data):

    # First download the file from the client
    fsize = int(data[data.find(ETX) + len(ETX):data.find(EOT)])
    fname = data[CCLEN:data.find(ETX)]
    print("Getting %s of size %d from %s" % (fname, fsize, str(sock.getpeername())))
    truepath = n.fp + fname
    f = open(truepath, 'wb')
    bytesrecv = 0
    while bytesrecv < fsize:
        newdata = sock.recv(BUFFER_SIZE)
        bytesrecv += len(newdata)
        f.write(newdata)
    print("File received!")

    # Send the file to all other clients
    host_add_file(n.fp, fname)


'''
Called when the host receives a delete request from a client node. Deletes the file and sends a command for all
other client nodes to delete that file as well
'''
def host_delete_file_from_client(sock, n, data):

    fname = data[CCLEN:data.find(EOT)] # Pull the file name from the command sent to us
    try:
        os.remove(files.get_filepath(fname))  # removes it locally
        for file in masterlist:
            if file.name == fname:
                masterlist.remove(file)
        print("File removed")
    except Exception as e:
        print("Error in host_delete_file_from_client: %s" % str(e))

    # Send delete command to all other nodes
    for conn in sock_list:
        message = DEL + fname + EOT
        print("Sending %s to %s" % (message, str(conn.getpeername())))
        conn.send(message.encode(ENCODING))
        time.sleep(0.01)
        print("Sent Successfully!")


'''
Called when the host node recevies a command to update a file from a client node. Downloads the updated file,
overwriting its current version and sends a command for all other nodes to do the same
'''
def host_update_file_from_client(sock, n, data):

    fname = data[CCLEN:data.find(ETX)]
    print("Updating %s from %s" % (fname, str(sock.getpeername())))

    # Override with the updated file into your directory
    host_add_file_from_client(sock, n, data)

    time.sleep(1)

    # Send the updated file to all the connections and have them override their file
    host_update_file(fname)


'''
Called when a new client connects with the host node. Sends all current files to the new client.
'''
def host_send_all_files(sock):
    global masterlist
    global sock_list

    message = "MAS"
    print("Sending: %s to %s" % (message, str(sock.getpeername())))
    sock.send(message.encode(ENCODING))
    time.sleep(0.1)
    resp = sock.recv(BUFFER_SIZE).decode(ENCODING)
    if resp == "OK":
        myfiles = files.scan()
        for file in myfiles:
            host_send_file(files.get_working_directory(), file.name)
            time.sleep(0.5)
    else:
        print("Didn't send Master List")


'''
Called as a thread to continuously accept incoming connections from new clients. Adds the new connection to our
master list of connections, sends all current files to the new client and creates a new thread to listen
for commands from each client.
'''
def host_accept(s, n):
    global sock_list

    print("Waiting to accept")
    while n.nodeOpen:
        conn, addr = s.accept()

        print("Connected with: %s" % str(conn.getpeername()))

        sock_list.append(conn)
        host_send_all_files(conn)

        t = threading.Thread(target=host_listen, args=(str(addr[0]), n, conn))
        t.start()


'''
Called by the host_accept thread to receive incomming commands from each client individually. Uses commands to 
add, update or delete a file from a client as well as disconnect and close the connection from a client. 
'''
def host_listen(name, n, sock):
    global sock_list

    while True:
        time.sleep(5)
        try:
            data = sock.recv(BUFFER_SIZE).decode(ENCODING)
        except socket.timeout:
            print("Time out in host_listen with: %s" % str(sock.getpeername()))
        if data:
            print(data)

        # each client can send one of the following to the host
        # new file - host downloads the file, host adds it to the masterlist, host sends the file to all other nodes (not back to the sender tho)
        # update file - host overwrites the file locally, host updates the mod time on the masterlist, host sends the overwrite message w/ file to all nodes (not back to sender)
        # delete file - host deletes the file from the masterlist, host deletes the message locally, host sends message to all nodes to delete their file with this name
        # disconnect - disconnects from the client and closes its connection. Removes connection from list of open connections

        if data[:CCLEN] == ADD:
            host_add_file_from_client(sock, n, data)
        if data[:CCLEN] == DEL:
            host_delete_file_from_client(sock, n, data)
        if data[:CCLEN] == UPD:
            host_update_file_from_client(sock, n, data)
        if data[:CCLEN] == DIS:
            print("Disconnecting from %s" % str(sock.getpeername()))
            sock_list.remove(sock)
            sock.close()
            break


'''
Display pop up message box when an error is thrown in the GUI
'''
def pop(msg='Something went wrong'):
    sg.popup('Error:', msg)


'''
GUI for the host to use the program. Has a window to display the current files available in the share folder and 5
options to add, delete, or update a file, open the file/folder and disconnect from the clients
'''
def host_console(name, n):
    sg.theme('Default1')

    layout = [
        [sg.Listbox(files.get_file_names(), size=(50, 10), key='list')],
        [sg.Button('Add File'),
         sg.Button('Update File'),
         sg.Button('Delete File'),
         sg.Button('Open File'),
         sg.Button('Disconnect')]
    ]

    hel = sg.Window('Pee2pee', layout)
    while True:

        event, values = hel.read(timeout=3000)

        hel['list'].update(files.get_file_names())

        if event is None:
            break

        if event == 'Add File':
            filepath = sg.popup_get_file('File to add')
            print(filepath)
            if filepath:
                host_add_file(filepath, os.path.basename(filepath))
                hel['list'].update(files.get_file_names())

        if event == 'Update File':
            filename = ''
            try:
                filename = values['list'][0]  # Throws an exception when nothing is selected, catch it here
            except:
                print("Make sure you have selected a file to update")
            if filename:
                host_update_file(filename)
                hel['list'].update(files.get_file_names())

        if event == 'Delete File':
            filename = ''
            try:
                filename = values['list'][0]  # Throws an exception when nothing is selected, catch it here
            except:
                print("Make sure you have selected a file to delete")
            if filename:
                host_delete_file(filename)
                hel['list'].update(files.get_file_names())

        if event == 'Open File':
            filename = ''
            try:
                filename = values['list'][0]  # Throws an exception when nothing is selected, catch it here
            except:
                print("Make sure you have selected a file to open")

            os.startfile(files.get_filepath(filename))

        if event == 'Disconnect':
            # delete connection
            host_close_connection()
            n.nodeOpen = False
            hel.close()
            break


'''
Called when the host disconnects. Sends a disconnect message to each client before closing the connection and removing
them from our list of connections
'''
def host_close_connection():
    global sock_list
    for sock in sock_list:
        print("Disconnecting from %s" % str(sock.getpeername()))
        sock.send(DIS.encode(ENCODING))
        sock_list.remove(sock)
        sock.close()


################################################################################


def client_node(n):  # creates the client socket and connects, then returns the socket
    s = socket.socket()
    s.connect((n.host, n.port))
    print("Connected to %s" % str(s.getpeername()))
    return s


def client_listen(name, sock,
                  n):  # the listener-thread, which receives messages from the host and calls the correct function.
    ##client listening for the host
    ##each client can recv one of the following from the host
    while True:
        try:
            data = sock.recv(BUFFER_SIZE).decode(ENCODING)
        except:
            data = ""
        if data:
            print(data)

        if data[:CCLEN] == MAS:
            sock.send("OK".encode(ENCODING))
        if data[:CCLEN] == ADD:  # adds a new file
            client_download_file(sock, n, data)
        if data[:CCLEN] == UPD:  # updates a file
            client_update_file_from_host(sock, n, data)
        if data[:CCLEN] == DEL:  # deletes a file
            client_delete_file_from_host(sock, n, data)
        if data[:CCLEN] == DIS:  # disconnects the client
            print("Socket closing with host")
            sock.close()
            break


def client_clear_folder():  # clears the folder on first connect
    shutil.rmtree(files.get_working_directory())
    check()


def client_delete_file_from_host(s, n, data):  # the logic for deleting a file that the host wants the client to delete
    fname = data[CCLEN:data.find(EOT)]
    print("Request to delete %s" % fname)
    currfiles = n.get_file_list()

    if fname not in currfiles:  # if they dont have the file, for some reason. never ran into this tho
        print("do nothing pretty much")
    else:
        time.sleep(0.1)
        print("Deleting file named: %s " % (fname))
        try:
            os.remove(files.get_filepath(fname))
        except Exception as e:
            print("Error in client_delete_file_from_host: %s" % str(e))


def client_delete_file(sock,
                       filename):  # the logic for when the client deletes a file and asks the host to delete it for everyone else too
    try:
        os.remove(files.get_filepath(filename))  # removes it locally
    except Exception as e:
        print("Error in client_delete_file: %s" % str(e))

    message = DEL + filename + EOT
    print("Sending: %s to %s" % (message, str(sock.getpeername())))
    sock.send(message.encode(ENCODING))
    time.sleep(0.01)
    print("Sent Successfully!")


def client_update_file(sock, name):  # logic for client updating a file and sending it out
    truepath = files.get_filepath(name)
    fsize = os.path.getsize(truepath)
    message = UPD + name + ETX + str(fsize) + EOT
    print("Sending: %s to %s" % (message, str(sock.getpeername())))
    sock.send(message.encode(ENCODING))
    time.sleep(0.01)
    with open(truepath, 'rb') as k:
        bytessent = 0
        while bytessent < fsize:
            data = k.read(BUFFER_SIZE)
            sock.send(data)
            bytessent += len(data)

        k.close()
        print("Sent Successfully!")


def client_update_file_from_host(sock, n, data):  # logic for client receiving an update request from the host
    fsize = int(data[data.find(ETX) + len(ETX):data.find(EOT)])
    fname = data[CCLEN:data.find(ETX)]

    currfiles = n.get_file_list()  # deletes the current version
    if fname in currfiles:
        try:
            os.remove(n.fp + fname)
        except Exception as e:
            print("Error in client_update_file_from_host: %s" % str(e))

    time.sleep(0.1)
    print("Upping file named: %s and of size: %d " % (fname, fsize))
    truepath = n.fp + fname
    f = open(truepath, 'wb')
    bytesrecv = 0
    while bytesrecv < fsize:
        newdata = sock.recv(BUFFER_SIZE)
        bytesrecv += len(newdata)
        f.write(newdata)
    print("File upped!")


def client_download_file(sock, n, data):  # receiving an add request from the host
    fsize = int(data[data.find(ETX) + len(ETX):data.find(EOT)])
    fname = data[CCLEN:data.find(ETX)]

    print("Receiving file named: %s and of size: %d " % (fname, fsize))
    truepath = n.fp + fname
    f = open(truepath, 'wb')
    bytesrecv = 0
    while bytesrecv < fsize:
        newdata = sock.recv(BUFFER_SIZE)
        bytesrecv += len(newdata)
        f.write(newdata)
    print("File received!")
    # else:
    #     # sock.send("NO".encode(ENCODING))
    #     pass


def client_add_file(sock, path, name):  # client adding a file and sending it to the host
    head, tail = os.path.split(path)
    try:
        shutil.move(path, os.path.join(files.get_working_directory(), tail))  # adds it to share folder
    except:
        print("Something went wrong dd4r")

    truepath = os.path.join(files.get_working_directory(), tail)
    fsize = os.path.getsize(truepath)
    message = ADD + name + ETX + str(fsize) + EOT
    print("Sending: %s to %s" % (message, str(sock.getpeername())))
    sock.send(message.encode(ENCODING))
    # time.sleep(0.1)
    # resp = sock.recv(BUFFER_SIZE).decode(ENCODING)

    print("They want it.")
    with open(truepath, 'rb') as k:
        bytessent = 0
        while bytessent < fsize:
            data = k.read(BUFFER_SIZE)
            sock.send(data)
            bytessent += len(data)

        k.close()
        print("Sent Successfully!")
    # else:
    #     print("They don't want it...")


def client_console(s, n):  # the main driver for client actions
    sg.theme('Default1')

    layout = [
        [sg.Listbox(files.get_file_names(), size=(50, 10), key='list')],
        [sg.Button('Add File'),
         sg.Button('Update File'),
         sg.Button('Delete File'),
         sg.Button('Open File'),
         sg.Button('Disconnect')
         ]

    ]
    hel = sg.Window('DoggyDoor', layout, icon=logo)
    while True:

        event, values = hel.read(timeout=3000)
        hel['list'].update(files.get_file_names())

        if event is None:
            break

        if event == 'Add File':  # logic for adding a file
            filepath = sg.popup_get_file('File to add')
            print(filepath)
            if filepath:
                try:
                    head, filename = os.path.split(filepath)
                    client_add_file(s, filepath, filename)
                    hel['list'].update(files.get_file_names())
                except socket.error as e:
                    if e.errno == errno.WSAECONNRESET:
                        s.close()
                        break

        if event == 'Update File':  # logic for updating a file
            filename = ''
            try:
                filename = values['list'][0]  # Throws an exception when nothing is selected, catch it here
            except:
                print("Make sure you have selected a file to update")
            if filename:
                try:
                    client_update_file(s, filename)
                    hel['list'].update(files.get_file_names())
                except socket.error as e:
                    if e.errno == errno.WSAECONNRESET:
                        s.close()
                        break

        if event == 'Delete File':  # logic for deleting a file
            filename = ''
            try:
                filename = values['list'][0]  # Throws an exception when nothing is selected, catch it here
            except:
                print("Make sure you have selected a file to delete")
            if filename:
                try:
                    client_delete_file(s, filename)
                    hel['list'].update(files.get_file_names())
                except socket.error as e:
                    if e.errno == errno.WSAECONNRESET:
                        s.close()
                        break

        if event == 'Open File':  # logic for opening a file
            filename = ''
            try:
                filename = values['list'][0]  # Throws an exception when nothing is selected, catch it here
            except:
                print("Make sure you have selected a file to open")

            os.startfile(files.get_filepath(filename))

        if event == 'Disconnect':  # logic for disconnecting from the host
            # delete connection
            message = DIS + EOT
            try:
                s.send(message.encode(ENCODING))  # sends a message that its disconnecting
            except Exception as e:
                print("Error in client_console: %s" % str(e))
            finally:
                s.close()
                hel.close()
            break

        hel['list'].update(files.get_file_names())


#############################################################################


def check():  # ensures that the proper files exist
    if not os.path.isdir(files.get_working_directory()):
        os.mkdir(files.get_working_directory())
    if not os.path.isdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'share')):
        os.mkdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'share'))
    if not os.path.isdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'temp')):
        os.mkdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'temp'))


def login_window():  # the main login window
    sg.theme('Default1')

    layout = [
        [sg.Text('Host IP:'), sg.InputText("192.168.2.151", key='ip', size=(30, 1))],
        # TODO get rid of the default text
        [sg.Text('Host Port:'), sg.InputText("8000", key='port', size=(20, 1))],
        [sg.Button('Connect to host')],
        [sg.Button('Host new connection')],
    ]

    window = sg.Window('DoggyDoor', layout, icon=logo)
    while True:

        try:
            event, values = window.read()
        except Exception as e:
            print("Error in login_window: %s" % str(e))
            break

        if event is None:
            break

        if event == 'Connect to host':  # become a client
            conn = None
            try:
                name = "User: " + str(uuid.uuid4())
                print(values['ip'] + ":" + values['port'])
                n = Node(name, 8000, values['ip'], int(values['port']))
                window.close()
                conn = client_node(n)
            except SystemExit:
                break
            except WindowsError as e:
                pop("Could not connect to host: " + str(e))
                login_window()
            except:
                break

            if conn:
                client_clear_folder()

                t = threading.Thread(target=client_listen, args=("client_listener", conn, n))
                t.start()

                client_console(conn, n)

        if event == 'Host new connection':  # become a host
            try:
                name = "User: " + str(uuid.uuid4())
                n = Node(name, 8000, values['ip'], int(values['port']))
                window.close()
                host_node(name, n)
            except Exception as e:
                pop("Could not host new connection: " + str(e))
                login_window()


def main():
    check()
    login_window()
    os._exit(0)  # end program


if __name__ == '__main__':
    main()