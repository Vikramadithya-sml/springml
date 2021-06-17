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
import requests
import json
import socket
import os
import os.path
from org.apache.nifi.processor.io import OutputStreamCallback
from org.python.core.util import StringUtil

url = "http://localhost:8080/nifi-api/"
update_state_url = "processors/8a069f8d-0179-1000-aaeb-eda86c6d6bda/run-status"
revision_url = "http://localhost:8080/nifi-api/processors/8a069f8d-0179-1000-aaeb-eda86c6d6bda"

payload = {}
headers = {}

class WriteContentCallback(OutputStreamCallback):
    def __init__(self, content):
        self.content_text = content

    def process(self, outputStream):
        try:
            outputStream.write(StringUtil.toBytes(self.content_text))
        except:
            traceback.print_exc(file=sys.stdout)
            raise

def update_processor_state():
    try:
        is_connected = check_connection()
        file_count = offline_directory_file_count()

        flowFile = session.create()

        print(is_connected, file_count)
        if is_connected and file_count > 0:
            print('Offline flow is initiated')
            update_state("RUNNING")
            flowFile = session.write(flowFile, WriteContentCallback(
                "Offline flow is initiated"))
        else:
            print('Offline flow is stopped')
            update_state("STOPPED")
            flowFile = session.write(flowFile, WriteContentCallback(
                "Offline flow is stopped"))

        session.transfer(flowFile, REL_SUCCESS)
        session.commit()

    except Exception as e:
        print('Error Updating Procesor State',e)
        flowFile = session.write(flowFile, WriteContentCallback(
                "Exception"))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()


def check_connection():

    IPaddress = socket.gethostbyname(socket.gethostname())
    if IPaddress == "127.0.0.1":
        print("No internet, your localhost is " + IPaddress)
        return False
    else:
        return True


def offline_directory_file_count():
    return len([name for name in os.listdir(r'D:\NiFi Processor Stopped')])


def update_state(state):
    revision = get_revision_data()
    payload = json.dumps({
        "revision": revision,
        "state": state
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "PUT", url+update_state_url, headers=headers, data=payload)
    print(response.text)


def get_revision_data():
    response = requests.request(
        "GET", revision_url, headers=headers, data=payload)
    return json.loads(response.text)['revision']


update_processor_state()
