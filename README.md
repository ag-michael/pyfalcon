# pyfalcon
Crowdstrike Falcon streaming api client in python. 

## Usage:

You can run the stand-alone `falcon_hose.py` script which will use the `config.json` json configuration file to connect tot he crowdstrike falcon streaming api. If you choose this method, you'll need to edit the script and modify the processor() function to process the returned data beyond printing it. 

Alternatively you can import it as a module as part of your own separate project.

Excrept of falcon_hose.py's code (for reference module usage):

```
# Initiate the streaming api, processor is a callback function which will handle json encoded streaming data objects
r=FalconStreamingAPI(config,processor)	
while True:
	try:
		#connect() will initiate the streaming api connection and fetch a list of streams, access tokens and expiration dates.
		while r.connect():
			#streamData() will start a new thread for each stream resource returned by connect(). the resulting threads will each be calling back your processor() callback.
			r.streamData()
	except Exception:
		continue	
```

