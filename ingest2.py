#!/usr/bin/env python
# read dump1090 data from a stream and store it in MongoDB.
# assumption: message timestamps are monotonically increasing.

import csv
import sys
import time
from datetime import datetime
from pytz import timezone
import signal
#from pymongo import MongoClient

############ CONFIG HERE ############
# your timezone
MY_TIMEZONE = 'Europe/Paris'
# flush interval (in seconds) when reading from stdin
FLUSH_INTERVAL = 10
# flush after reading this number of useful messages (not lines) from a file
FLUSH_MESSAGE_COUNT = 50
# database config
# database config
DATABASE = 'adsb'
COLLECTION = 'readings'
URI = 'mongodb://localhost:27017/'
############ END CONFIG #############

def flush_buffer():
        bulk = collection.initialize_unordered_bulk_op()
        for key in buffer:
                insert_doc = {"icao": buffer[key]['icao'], "t": buffer[key]['ts']}
                if 'callsign' in buffer[key]:
                        insert_doc['callsign'] = buffer[key]['callsign']
                if 'events' in buffer[key] and len(buffer[key]['events']) > 0:
                        for event in buffer[key]['events']:
                                baseline = insert_doc.copy()
                                baseline.update(event) # overwrite t
                                bulk.insert(baseline)
                else:
                        bulk.insert(insert_doc)
        result = bulk.execute()
        print("++")
        buffer.clear()

def fullfllush_buffer():
        print("++flushing++")
        #bulk = collection.initialize_unordered_bulk_op()
        #for key in buffer:
        #       insert_doc = {"icao": buffer[key]['icao'], "t": buffer[key]['ts']}
        #       if 'callsign' in buffer[key]:
        #        insert_doc['callsign'] = buffer[key]['callsign']
        #        if 'events' in buffer[key] and len(buffer[key]['events']) > 0:
        #                for event in buffer[key]['events']:
        #                        baseline = insert_doc.copy()
        #                        baseline.update(event) # overwrite t
        #                        bulk.insert(baseline)
        #        else:
        #                bulk.insert(insert_doc)
        #result = bulk.execute()
        buffer.clear()

def catch_sigint(signal, frame):
        fullflush_buffer()
        sys.exit(0)

# database initialization
#dbclient = MongoClient(URI)
#db = dbclient[DATABASE]
#collection = db[COLLECTION]

# variable initialization
buffer = {}
signal.signal(signal.SIGINT, catch_sigint)
messages_read = 0
vitesse = 0
alt = 1000 
my_tz = timezone(MY_TIMEZONE)
utc_tz = timezone('UTC')
from_network = True if 1 == len(sys.argv) else False # note: you can use '-' if you want to uncompress the input

# main loop
with sys.stdin if from_network or sys.argv[1] is '-' else open(sys.argv[1], 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        start = time.time()
        for row in reader:
                msg_type, transmission_type, session_id, aircraft_id, icao_hex, flight_id, date_gen, time_gen, date_logged, time_logged, callsign, altitude, speed, bearing, latitude, longitude, vertical_rate, squawk, alert,emergency, spi, onground = row

                if altitude:
                    alt = float(altitude)
                if speed:
                    vitesse = float(speed)
                if (onground == 0):
                   print("row: " + row)
                if (transmission_type == 2):
                   print("row: " + row)
                if (altitude) and (speed) and (alt > 0):
                   print("")
                   print("--------------------------------------")
                   print("-->> alt= " + altitude + " vitesse= " + speed + " icao= " + icao_hex + " callsign= " + callsign + " onground= " + onground)
                   print("+++++message type is " + msg_type + " tran type " + transmission_type)
                #   print("--------------------------------------")
                #   print("")
                #if callsign:
                   #print("--> icao=" + icao_hex + " callsign=" + callsign + " onground= " + onground)
                if ("MSG" != msg_type) or (transmission_type in ["2"] and (onground == "-1") and (speed) and (vitesse > 50)):
                   print("//////////        Landing       ///////////")
                   print("type -->" + transmission_type + "-->> alt= " + altitude + " icao= " + icao_hex + " speed= " + speed + " onground= " + onground)
                   print("row: " + str(row))
                if ("MSG" != msg_type) or (transmission_type in ["2"] and (onground == "0") and (vitesse > 90) and (alt <1900)):
                   print("//////////        Take-off       ///////////")
                   print("type -->" + transmission_type + "-->> alt= " + altitude + " icao= " + icao_hex + " speed= " + speed + " onground= " + onground)
                   print("row: " + str(row))
                if ("MSG" != msg_type) or (transmission_type in ["2"]):
                        continue
                
                local_dt_ms = my_tz.localize(datetime.strptime(date_gen + " " + time_gen, "%Y/%m/%d %H:%M:%S.%f"))
                utc_dt_second = local_dt_ms.replace(microsecond = 0).astimezone(utc_tz) # UTC to the nearest second
                utc_str_hour = local_dt_ms.replace(microsecond = 0, second = 0, minute = 0).astimezone(utc_tz).strftime("%Y%m%d%H") # UTC to the nearest hour
                _id = utc_str_hour + ":" + icao_hex
                # initialize the dictionary
                try:
                        buffer[_id]['icao'] = icao_hex
                        buffer[_id]['speed'] = vitesse 
                except (KeyError):
                        buffer.setdefault(_id, {'icao': icao_hex})
                        # 'ts' is the timestamp of the first message recorded in a given document,
                        # which may precede the timestamp in the first 'event' array element.
                        # The following assignment is not idempotent, but we rectify during initial insert.
                        buffer[_id]['ts'] = utc_dt_second

                # process each message type
                if (transmission_type in ["2"] and (onground == "-1") and (speed) and (vitesse > 50)):
                        print("-----------------------------------------------------------------------------------")
                        print("Landing SWITCH vitesse: " + speed + "icao: " + icao_hex + " time: "+ time_logged)
                        print("-----------------------------------------------------------------------------------")

                if from_network:
                        now = time.time()
                        if now - start >= FLUSH_INTERVAL:
                            fullfllush_buffer()
                            start = now
                else:
                        messages_read = messages_read + 1
                        if messages_read >= FLUSH_MESSAGE_COUNT:
                                fullfllush_buffer()
                                messages_read = 0

# finally flush the buffer if we were reading from a file
fullfllush_buffer()