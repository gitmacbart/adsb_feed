#!/usr/bin/env python
# read dump1090 data from a stream and store it in MongoDB.
# assumption: message timestamps are monotonically increasing.

import csv
import sys
import time
from datetime import datetime
from pytz import timezone
import signal


############ CONFIG HERE ############
# your timezone
MY_TIMEZONE = 'Europe/Paris'

############ END CONFIG #############


def catch_sigint(signal, frame):
	sys.exit(0)

# variable initialization
signal.signal(signal.SIGINT, catch_sigint)
vitesse = 0
alt = 1000 
my_tz = timezone(MY_TIMEZONE)
utc_tz = timezone('UTC')
from_network = True if 1 == len(sys.argv) else False # note: you can use '-' if you want to uncompress the input

# main loop
with sys.stdin as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        start = time.time()
        for row in reader:
                msg_type, transmission_type, session_id, aircraft_id, icao_hex, flight_id, date_gen, time_gen, date_logged, time_logged, callsign, altitude, speed, bearing, latitude, longitude, vertical_rate, squawk, alert,emergency, spi, onground = row

                if altitude:
                    alt = float(altitude)

                if speed:
                    vitesse = float(speed)

                if ("MSG" != msg_type) or (transmission_type in ["2"] and (onground == "-1") and (speed) and (vitesse > 50)):
                   print("---------------------------------------------------------------------------")
                   print("--> Landing  ICAO : " + icao_hex)
                   print("Time Of Arrival: " + time_gen + "  Date Of Arrival: " + date_gen)
                   print("---------------------------------------------------------------------------")

                if ("MSG" != msg_type) or ((transmission_type in ["3"]) and (onground == "0") and (altitude) and (alt == 1500)):
                   print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                   print("--> Take-Off  ???????? ICAO : " + icao_hex)
                   print("Time Of Departure: " + time_gen + "  Date Of Departure: " + date_gen)
                   print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                   print(" ")
                   print("row: " + str(row))
                   print(" ")
                   
                if ("MSG" != msg_type) or ((transmission_type in ["3","2"]) and (onground == "0") and (icao_hex == "3C5C41XX")):
                   print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                   print("--> xxxxxxxxxx : " + icao_hex)
                   print("Message: " + transmission_type)
                   print("row: " + str(row))
                   print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
# finally flush the buffer if we were reading from a file