# -*- coding: utf-8 -*-
import requests
import json
import datetime 
import hashlib
import hmac
import base64
import collections
import socket
import ssl 
import threading
import traceback
import time
import sys
from requests import Request, Session
from requests_toolbelt.utils import dump

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
			except:
				traceback.print_exc()
				continue

class FalconStreamingAPI:
	def __init__(self,config,processor):
		self.config=config
		self.key=config["falcon_api_key"]
		self._id = config["falcon_api_id"]
		self.Method="GET"
		self.md5=''
		self.url=config["falcon_data_feed_url"]+"?appId="+config["client_name"]
		self.date=datetime.datetime.utcnow().strftime("%a, %d %b %Y %X GMT")
		self.RequestUri_Host=config["falcon_hose_domain"]
		self.RequestUri_AbsolutePath='/sensors/entities/datafeed/v1'
		self.RequestUri_Query='?appId='+config["client_name"]
		self.Headers={}
		self.processor=processor

	def calculateHMAC(self,_key,_requestString):
		digest=hmac.new(str(_key),msg=str(_requestString),digestmod=hashlib.sha256).digest()
		return base64.b64encode(digest)

	def CanonicalQueryString(self,qstr):
		return qstr.split('?')[1]

	def connect(self):
		try:
			requestString = self.Method+"\n"+self.md5+"\n"+self.date+"\n"+self.RequestUri_Host+self.RequestUri_AbsolutePath+"\n"+self.CanonicalQueryString(self.RequestUri_Query)
			signature = self.calculateHMAC(self.key,requestString)
			self.Headers["X-CS-Date"] = self.date
			self.Headers["Authorization"] = "cs-hmac " + self._id + ":" + signature + ":customers"
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			context = ssl.create_default_context()
			c = context.wrap_socket(socket.socket(socket.AF_INET), server_hostname=self.config["falcon_hose_domain"])
			c.connect((self.config["falcon_hose_domain"], 443))
			rs="GET /sensors/entities/datafeed/v1?appId="+self.config["client_name"]+" HTTP/1.1\n"
			rs+="X-CS-Date: "+self.date+"\n"
			rs+="Authorization: "+self.Headers["Authorization"]+"\n"
			rs+="Host: "+self.config["falcon_hose_domain"]+"\n"
			rs+="Connection: Keep-Alive\r\n\r\n"
			rs=rs.encode('utf-8')

			c.sendall(rs)
			data = c.recv(10000)
			body=data.split('\r\n\r\n')[1]
			self.stream_resources=json.loads(body)["resources"]
			c.close()
		except Exception:
			traceback.print_exc()
			return False
		return True


	def streamData(self):
		for i in range(len(self.stream_resources)):
			ds_headers={"Authorization":"Token "+self.stream_resources[0]["sessionToken"]["token"],"Accepts":"appication/json"}
			stream_processor=StreamProcessor(requests.get(self.stream_resources[0]["dataFeedURL"],headers=ds_headers,stream=True),self.processor)
			stream_processor.setDaemon(True)
			stream_processor.start()

def processor(stream_data):
	print(stream_data)
def main():
	config={}
	with open(sys.argv[1]) as f:
		config=json.loads(f.read())
	r=FalconStreamingAPI(config,processor)	
	while True:
		try:
			while r.connect():
				r.streamData()
		except Exception:
			traceback.print_exc()
			time.sleep(3)
			continue		


if __name__ == "__main__":
	reload(sys)
	sys.setdefaultencoding("utf-8")
	main()

