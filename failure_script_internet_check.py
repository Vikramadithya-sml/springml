###############################################################################
# Setting flowfile attributes in ExecuteScript
#
# Variables provided in scope by script engine:
#
#    session - ProcessSession
#    context - ProcessContext
#    log - ComponentLog
#    REL_SUCCESS - Relationship
#    REL_FAILURE - Relationship
###############################################################################
import socket

url = "http://localhost:8080/nifi-api/"

payload = {}
headers = {}


def check_connection():
    IPaddress = socket.gethostbyname(socket.gethostname())
    if IPaddress == "127.0.0.1":
        print("No internet, your localhost is " + IPaddress)
        return False
    else:
        return True
    # sock = socket.create_connection(("www.google.com", 80))
    # if sock is not None:
    #     print('Clossing socket')
    #     sock.close
    #     return True
    # else:
    #     return False


def start_failure_script():
    flowFile = session.get()
    # flowFile = ''
    if flowFile != None:

        # Set single attribute
        try:

            sock = check_connection()
            if sock:
                flowFile = session.putAttribute(flowFile, "isaConnected", "yes")
            else:
                flowFile = session.putAttribute(flowFile, "isaConnected", "no")
                session.transfer(flowFile, REL_SUCCESS)
                return

            session.transfer(flowFile, REL_SUCCESS)
            session.commit()

        except Exception as e:
            # flowFile = session.putAttribute(flowFile, "isaConnected", "no")
            # session.transfer(flowFile, REL_SUCCESS)
            session.rollback()
start_failure_script()