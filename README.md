# pyfalcon
Crowdstrike Falcon streaming api client in python. 

## Usage:

You can run the stand-alone `pyfalcon.py` script which will use the `config.json` json configuration file to connect to the crowdstrike falcon streaming api. If you choose this method, you'll need to edit the script and modify the processor() function to process the returned data beyond printing it. 

Alternatively you can import it as a module as part of your own separate project.

Example module usage:

```
import pyfalcon
def processor(stream_object):
        print(stream_object)
#Fill in the configuration items as needed.        
config={
	"client_name":"<your client name>",
	"falcon_hose_domain":"firehose.crowdstrike.com",
	"falcon_data_feed_url":"https://firehose.crowdstrike.com/sensors/entities/datafeed/v1?appId=",
	"falcon_api_id":"<api_id>",
	"falcon_api_key":"<api_key>"


}
# Initiate the streaming api, processor is a callback function which will handle json encoded streaming data objects

r=pyfalcon.FalconStreamingAPI(config,processor)
while True:
        try:
                #connect() will initiate the streaming api connection and fetch a list of streams, access tokens and expiration dates.
                while r.connect():
                        #streamData() will start a new thread for each stream resource returned by connect(). the resulting threads will each be calling back your processor() callback.
                        r.streamData()
        except Exception:
                continue
```

