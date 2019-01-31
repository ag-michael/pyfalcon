#!/bin/python2
# -*- coding: utf-8 -*-
import requests
import json
import time
import calendar
import datetime
import hashlib
import hmac
import base64
import collections
import socket
import ssl
import threading
import traceback
import logging

import sys
from requests import Request, Session


class StreamProcessor(threading.Thread):
    def __init__(self, stream, stream_resource, processor, logger, offsets_file="./offsets_file.json"):
        threading.Thread.__init__(self)
        self.stream = stream
        self.stream_resource = stream_resource
        self.processor = processor
        self.lh = logger
        self.running = True
        self.offsets = {}
        self.offsets_file = offsets_file
        try:
            with open(offsets_file, "w+") as of:
                data = of.read()
                if data and data.strip():
                    offsets = json.loads()
                else:
                    self.offsets[self.stream_resource["dataFeedURL"]] = 0
                    of.write(json.dumps(self.offsets))
        except:
            self.lh.exception("offset file error")

    def run(self):
        self.lh.debug("Started a StreamProcessor thread")
        try:
            while self.running:
                for stream_data in self.stream.iter_lines():
                    if stream_data.strip():
                        try:
                            json_data = json.loads(stream_data)
                            offset = json_data["metadata"]["offset"]
                            if offset > self.offsets[self.stream_resource["dataFeedURL"]]+100:
                                self.offsets[self.stream_resource["dataFeedURL"]] = offset
                                with open(self.offsets_file, "w+") as of:
                                    of.write(json.dumps(self.offsets))

                            self.processor(json_data)
                        except ValueError:
                            self.lh.exception("Can't decode:\n"+stream_data)
                            continue
                        except Exception as e:
                            self.lh.exception(str(e))
                            traceback.print_exc()
                            continue
        except Exception as e:
            self.lh.exception("Stream processor thread exception:"+str(e))
        self.lh.debug("StreamProcessor exiting for stream:\n" +
                      self.stream_resource["dataFeedURL"])


class FalconStreamingAPI:
    def __init__(self, config, processor, logger=None):
        if not logger:
            self.lh = logging.getLogger('PyFalcon')
            self.lh.setLevel(logging.DEBUG)
            logging.basicConfig(format='PyFalcon: %(asctime)-15s  %(message)s')
            self.lh.info("Starting Falcon streaming api script...")
        else:
            self.lh = logger
        self.config = config
        self.key = config["falcon_api_key"]
        self._id = config["falcon_api_id"]
        self.Method = "GET"
        self.md5 = ''
        self.url = config["falcon_data_feed_url"] + \
            "?appId="+config["client_name"]
        self.RequestUri_Host = config["falcon_hose_domain"]
        self.RequestUri_AbsolutePath = '/sensors/entities/datafeed/v1'
        self.RequestUri_Query = '?appId='+config["client_name"]
        self.Headers = {}
        self.processor = processor
        self.reconnect = True
        self.sleeptime = 300
        if "offsets_file" in config:
            self.offsets_file = config["offsets_file"]
        else:
            self.offsets_file = "./offsets.json"

    def calculateHMAC(self, _key, _requestString):
        digest = hmac.new(str(_key), msg=str(_requestString),
                          digestmod=hashlib.sha256).digest()
        return base64.b64encode(digest)

    def CanonicalQueryString(self, qstr):
        return qstr.split('?')[1]

    def connect(self):
        try:
            self.date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %X GMT")
            self.lh.debug(
                "Connecting to the streaming api with date stamp:"+self.date)
            requestString = self.Method+"\n"+self.md5+"\n"+self.date+"\n"+self.RequestUri_Host + \
                self.RequestUri_AbsolutePath+"\n" + \
                self.CanonicalQueryString(self.RequestUri_Query)
            signature = self.calculateHMAC(self.key, requestString)
            self.Headers["X-CS-Date"] = self.date
            self.Headers["Authorization"] = "cs-hmac " + \
                self._id + ":" + signature + ":customers"
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            context = ssl.create_default_context()
            c = context.wrap_socket(socket.socket(
                socket.AF_INET), server_hostname=self.config["falcon_hose_domain"])
            self.lh.debug("Connecting to Falcon streaming API using TLS.")
            c.connect((self.config["falcon_hose_domain"], 443))
            rs = "GET /sensors/entities/datafeed/v1?appId=" + \
                self.config["client_name"]+" HTTP/1.1\n"
            rs += "X-CS-Date: "+self.date+"\n"
            rs += "Authorization: "+self.Headers["Authorization"]+"\n"
            rs += "Host: "+self.config["falcon_hose_domain"]+"\n"
            rs += "Connection: Keep-Alive\r\n\r\n"
            rs = rs.encode('utf-8')

            c.sendall(rs)
            data = c.recv(10000)
            c.close()

            body = data.split('\r\n\r\n')[1]

            self.data_stream = json.loads(body)
            if "errors" in self.data_stream:
                self.lh.debug("Errors in data stream response:\n" +
                              json.dumps(self.data_stream, indent=4, sort_keys=True))
                self.reconnect = True
                raise

            elif self.data_stream["meta"]["pagination"] and "total" in self.data_stream["meta"]["pagination"] and self.data_stream["meta"]["pagination"]["total"] > 0:
                if "resources" in self.data_stream:
                    self.stream_resources = self.data_stream["resources"]
                    self.lh.info(
                        "Discovered "+str(len(self.stream_resources))+" stream resources.")
                    self.reconnect = False
                else:
                    self.lh.debug(
                        "No resrouces:\n"+json.dumps(str(body), indent=4, sort_keys=True))
                    self.reconnect = False
                    return False
                until = 300
                for stream in self.stream_resources:
                    expiration = stream['sessionToken']['expiration']
                    expires = calendar.timegm(time.strptime(
                        expiration[:len(expiration)-4]+"Z", "%Y-%m-%dT%H:%M:%S.%fZ"))
                    now = time.time()
                    if expires-now < until:
                        until = expires-now  # re-discover streams after waiting for the shortest token expiry time
                        self.expires = expires

                self.lh.debug("Rediscovering streams in:"+str(until))
                if until > 295:
                    self.sleeptime = until  # get new token after expiry
                else:
                    # sessions are supposed to be long lived
                    self.lh.debug("Short token expiry!:"+str(until))
                    self.sleeptime = 300  # wait 5 min before new token anyways

                self.lh.debug("New Expiration:"+expiration)
            elif self.data_stream["meta"]["pagination"] and "total" in self.data_stream["meta"]["pagination"] and self.data_stream["meta"]["pagination"]["total"] == 0:
                self.lh.debug("Discover attempt resulted in 0 resources")
                self.reconnect = False
                return False
            else:
                self.lh.debug("Unknown response:\n"+str(self.data_stream))
                raise
                self.expires = time.time()+60  # no resources, retry after 60secs
                self.reconnect = False
        except Exception as e:
            traceback.print_exc()
            self.lh.exception(str(e))
            self.reconnect = True
            return False
        return True

    def streamData(self):
        offsets = None
        try:
            try:
                with open(self.offsets_file) as of:
                    offsets = json.loads(of.read())
            except:
                pass
            for i in range(len(self.stream_resources)):
                ds_headers = {"Authorization": "Token " +
                              self.stream_resources[i]["sessionToken"]["token"], "Accepts": "appication/json"}
                self.lh.info("Opening stream for data feed:" +
                             self.stream_resources[i]["dataFeedURL"])
                offset = 0
                if offsets and self.stream_resources[i]["dataFeedURL"] in offsets:
                    offset = offsets[self.stream_resources[i]["dataFeedURL"]]
                request_url = self.stream_resources[i]["dataFeedURL"] + \
                    "&offset="+str(offset)
                self.lh.debug("DS request URL:"+request_url)
                response = requests.get(
                    request_url, headers=ds_headers, stream=True)
                if response.status_code == 200:
                    stream_processor = StreamProcessor(
                        response, self.stream_resources[i], self.processor, self.lh, offsets_file=self.offsets_file)
                    stream_processor.setDaemon(True)
                    stream_processor.start()
                    time.sleep(3)
                    if stream_processor.isAlive():
                        self.lh.debug("Started a new  stream processor thread")
                    else:
                        self.lh.debug("Stream processor thread is not alive.")
                else:
                    self.lh.error(
                        "Error opening stream '"+self.stream_resources[i]["dataFeedURL"]+"':\n"+response.text)
                    continue
            self.stream_resources = []
        except Exception as e:
            self.lh.exception(str(e))


def processor(stream_data):
    print(stream_data)


def main():
    config = {}
    with open(sys.argv[1]) as f:
        config = json.loads(f.read())
    r = FalconStreamingAPI(config, processor)
    while True:
        try:
            sleeptime = 300

            if r.connect():
                r.streamData()
                print("sleeping for "+str(sleeptime)+" seconds.")

                time.sleep(sleeptime)
            if not r.reconnect:
                time.sleep(sleeptime)
        except Exception:
            traceback.print_exc()
            time.sleep(3)
            continue


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")
    main()

