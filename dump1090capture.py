# capture_dump1090, Python script to capture aircraft.json data to a CSV file
# every one second.


#import urllib.request      #for Python 3
import urllib2              #for Python 2.7

import json
import os
import time
from time import gmtime, strftime
from datetime import datetime

url = 'file:///run/dump1090-fa/aircraft.json'        # where to get the aircraft.json
ADSBDir = '/mnt/sm-7/ADS-B Data/'
#'/home/pi/'                   # where to save the CSV files

headers = {'Cache-Control':'no-cache','Pragma':'no-cache','If-Modified-Since':'Sat, 1 Jan 2000 00:00:00 GMT',}

while 1:

    #use next two lines if you are using Python 3
    #request=urllib.request.Request(url, None, headers)
    #response = urllib.request.urlopen(request)

    #use next two lines if you are using Python 2.7
    request=urllib2.Request(url, None, headers)
    response = urllib2.urlopen(request)

    data = response.read().decode('utf8')
    jsonPlane = json.loads(data)

    # CSV file named as dump1090_YY-MM-DD.csv using UTC time
    fileName = ADSBDir+'dump1090_'+datetime.utcnow().strftime("%Y-%m-%d")+'.csv'

    if os.path.isfile(fileName):
        ADSBFile = open(fileName, 'a')
    else:
        ADSBFile = open(fileName, 'w')
        ADSBFile.write('UTC,icao,flight,spd,track,lat,lon,alt_geom,alt_baro,squawk,emergency,mlat\n')       #header

    for plane in jsonPlane['aircraft']:

        resultStr = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + ','

        if int(plane['seen']) <= 1:                         # planes that have been seen within the last one second
            if 'seen_pos' in plane:                         # if it's been seen, let's save some data

                # if additional data is wanted, add it below and adjust the header

                #ICAO number (uniqely identifies plane)
                if 'hex' in plane:
                    resultStr = resultStr +str(plane['hex']) + ','
                else:
                    resultStr = ','

                #Callsign / indent
                if 'flight' in plane:
                    resultStr = resultStr + str(plane['flight']).strip() + ','
                else:
                    resultStr = resultStr + ','

                #ground speed in knots (conversion to mph = *1.151)
                if 'gs' in plane:
                    resultStr = resultStr + str(plane['gs']) + ','
                else:
                    resultStr = resultStr + ','
                    
                #track of plane in degrees
                if 'track' in plane:
                    resultStr = resultStr +str(plane['track']) + ','
                else:
                    resultStr = ','

                #latitude
                if 'lat' in plane:
                    resultStr = resultStr + str(plane['lat']) + ','
                else:
                    resultStr = resultStr + ','

                #longitude
                if 'lon' in plane:
                    resultStr = resultStr + str(plane['lon']) + ','
                else:
                    resultStr = resultStr +','

                #geometric altitude
                if 'alt_geom' in plane:
                    resultStr = resultStr + str(plane['alt_geom']) + ','
                else:
                    resultStr = resultStr +','
                
                #Barometric altitude
                if 'alt_baro' in plane:
                    resultStr = resultStr + str(plane['alt_baro']) + ','
                else:
                    resultStr = resultStr +','
                
                #Squawk number
                if 'squawk' in plane:
                    resultStr = resultStr + str(plane['squawk']) + ','
                else:
                    resultStr = resultStr +','
                
                #Emergency?
                if 'emergency' in plane:
                    resultStr = resultStr + str(plane['emergency']) + ','
                else:
                    resultStr = resultStr +','

                #Is MLAT enabled?
                if 'mlat' in plane:
                    resultStr = resultStr + 'True'
                else:
                    resultStr = resultStr + 'False'

                ADSBFile.write(resultStr+'\n')

                # comment the below if you do not want to see on the console what is being saved
                #print (resultStr)
    ADSBFile.close()
    #print(jsonPlane)
    time.sleep(1)