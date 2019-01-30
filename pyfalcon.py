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
	def __init__(self,request_stream,processor):
		threading.Thread.__init__(self) 
		self.stream=request_stream
		self.processor=processor
	def run(self):
		for stream_data in self.stream.iter_lines():
			try:
				json_data=json.loads(stream_data)
				self.processor(json_data)
			except ValueError:
				continue
			except:
				traceback.print_exc()
				continue

class FalconStreamingAPI:
	def __init__(self,config,processor,logger=None):
		if not logger:
				self.lh = logging.getLogger('PyFalcon')
				self.lh.setLevel(logging.DEBUG)
				logging.basicConfig(format='PyFalcon: %(asctime)-15s  %(message)s')
				self.lh.info("Starting Falcon streaming api script...")
		else:
			self.lh=logger
		self.config=config
		self.key=config["falcon_api_key"]
		self._id = config["falcon_api_id"]
		self.Method="GET"
		self.md5=''
		self.url=config["falcon_data_feed_url"]+"?appId="+config["client_name"]
		self.RequestUri_Host=config["falcon_hose_domain"]
		self.RequestUri_AbsolutePath='/sensors/entities/datafeed/v1'
		self.RequestUri_Query='?appId='+config["client_name"]
		self.Headers={}
		self.processor=processor
		self.reconnect=True
		self.sleeptime=300

	def calculateHMAC(self,_key,_requestString):
		digest=hmac.new(str(_key),msg=str(_requestString),digestmod=hashlib.sha256).digest()
		return base64.b64encode(digest)

	def CanonicalQueryString(self,qstr):
		return qstr.split('?')[1]

	def connect(self):
		try:
			self.date=datetime.datetime.utcnow().strftime("%a, %d %b %Y %X GMT")
			self.lh.debug("Connecting to the streaming api with date stamp:"+self.date)
			requestString = self.Method+"\n"+self.md5+"\n"+self.date+"\n"+self.RequestUri_Host+self.RequestUri_AbsolutePath+"\n"+self.CanonicalQueryString(self.RequestUri_Query)
			signature = self.calculateHMAC(self.key,requestString)
			self.Headers["X-CS-Date"] = self.date
			self.Headers["Authorization"] = "cs-hmac " + self._id + ":" + signature + ":customers"
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			context = ssl.create_default_context()
			c = context.wrap_socket(socket.socket(socket.AF_INET), server_hostname=self.config["falcon_hose_domain"])
			self.lh.debug("Connecting to Falcon streaming API using TLS.")
			c.connect((self.config["falcon_hose_domain"], 443))
			rs="GET /sensors/entities/datafeed/v1?appId="+self.config["client_name"]+" HTTP/1.1\n"
			rs+="X-CS-Date: "+self.date+"\n"
			rs+="Authorization: "+self.Headers["Authorization"]+"\n"
			rs+="Host: "+self.config["falcon_hose_domain"]+"\n"
			rs+="Connection: Keep-Alive\r\n\r\n"
			rs=rs.encode('utf-8')

			c.sendall(rs)
			data = c.recv(10000)
			c.close()

			body=data.split('\r\n\r\n')[1]
			
			self.data_stream=json.loads(body)
			if "errors" in self.data_stream:
				self.lh.debug("Errors in data stream response:\n"+json.dumps(self.data_stream,indent=4,sort_keys=True))
				self.reconnect=True
				raise
			elif self.data_stream["meta"]["pagination"] and "total" in self.data_stream["meta"]["pagination"] and self.data_stream["meta"]["pagination"]["total"]>0:
				if "resources" in self.data_stream:
					self.stream_resources=self.data_stream["resources"]
					self.lh.info("Discovered "+str(len(self.stream_resources))+" stream resources.")
					self.reconnect=False
				else:
					self.lh.debug("No resrouces:\n"+json.dumps(str(body),indent=4,sort_keys=True))
					self.reconnect=False
					return False
				until=300
				for stream in self.stream_resources:
					expiration=stream['sessionToken']['expiration']
					expires=calendar.timegm(time.strptime(expiration[:len(expiration)-4]+"Z","%Y-%m-%dT%H:%M:%S.%fZ"))
					now=time.time()
					if expires-now < until:
						until=expires-now #re-discover streams after waiting for the shortest token expiry time
						self.expires=expires
	
				self.lh.debug("Rediscovering streams in:"+str(until))
				if until>295:
					self.sleeptime=until #get new token after expiry
				else:
					self.lh.debug("Short token expiry!:"+str(until)) # sessions are supposed to be long lived
					self.sleeptime=300 #wait 5 min before new token anyways

				self.lh.debug("New Expiration:"+expiration)
			elif self.data_stream["meta"]["pagination"] and "total" in self.data_stream["meta"]["pagination"] and self.data_stream["meta"]["pagination"]["total"]==0:
				self.lh.debug("Discover attempt resulted in 0 resources")
				self.reconnect=False
				return False
			else:
				self.lh.debug("Unknown response:\n"+str(self.data_stream))
				raise
				self.expires=time.time()+60 #no resources, retry after 60secs
				self.reconnect=False
		except Exception as e:
			traceback.print_exc()
			self.lh.exception(str(e))
			self.reconnect=True
			return False
		return True


	def streamData(self):
		for i in range(len(self.stream_resources)):
			ds_headers={"Authorization":"Token "+self.stream_resources[i]["sessionToken"]["token"],"Accepts":"appication/json"}
			self.lh.info("Opening stream for data feed:"+self.stream_resources[i]["dataFeedURL"])
			response=requests.get(self.stream_resources[i]["dataFeedURL"],headers=ds_headers,stream=True)
			if response.status_code==200:
				stream_processor=StreamProcessor(response,self.processor)
				stream_processor.setDaemon(True)
				stream_processor.start()
				self.lh.debug("Started a new daemon stream processor thread")
			else:
				self.lh.error("Error opening stream '"+self.stream_resources[i]["dataFeedURL"]+"':\n"+response.text)
				continue
		self.stream_resources=[]

def processor(stream_data):
	print(stream_data)
def main():
	config={}
	with open(sys.argv[1]) as f:
		config=json.loads(f.read())
	r=FalconStreamingAPI(config,processor)	
	while True:
		try:
			r.sleeptime=300#self.expires-time.time()

			if r.connect():
				r.streamData()
				print("sleeping for "+str(r.sleeptime)+" seconds.")
				
				time.sleep(r.sleeptime)
			if not r.reconnect:
				time.sleep(r.sleeptime) 
		except Exception:
			traceback.print_exc()
			time.sleep(3)
			continue		


if __name__ == "__main__":
	reload(sys)
	sys.setdefaultencoding("utf-8")
	main()

