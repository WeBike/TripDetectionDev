import math
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt



#http://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def haversine(lat1, lat2, lon1, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 

    # 6367 km is the radius of the Earth
    km = 6367 * c
    return abs(km)





def detectTrips(dbc,imei,startDate,endDate):
    stmt = "select * from imei{0} where stamp >= \"{1}\" and stamp <= \"{2}\" and latgps is not null and longgps is not null order by stamp".format(imei, startDate, endDate)
    lastRow = ""
    lastRowTime = ""
    
    tripHasStarted = 0
    tripEarlyStart = 0
    tripLateStart = 0
    tripStart = 0
    tripEarlyEnd = 0
    tripLateEnd = 0
    tripEnd = 0
    secondsSinceLastSignificantMovement = 0
    minTripDist = 0
    maxTripDist = 0
    tripDist = 0
    
    tripEarliestStartTimes = []
    tripLatestStartTimes = []
    tripStartTimes = []  # old non-interval start times
    tripEarliestEndTimes = []
    tripLatestEndTimes = []
    tripEndTimes = [] # old non-interval end times
    tripShortestDists = []
    tripLongestDists = []
    tripDists = [] # old non-interval distances
    tripLongestDurations = []
    tripShortestDurations = []
    
    realLastRowTime = ""
    wasMissingGPS = False
    lastMagX = 0
    magXDelta = 0
    wasMotionPresent = False
    isCharging = False

    lastMagXList = []
    lastMagXTime = ""
    for l in dbc.SQLSelectGenerator(stmt):
        if l[10] != 0:
            lastMagXList.append(abs(l[10] - lastMagX))
        if len(lastMagXList) > 30:
            if tripHasStarted == 1 and wasMotionPresent == False:
                for i in range(len(lastMagXList)):
                    for j in range(len(lastMagXList) - i):
                        if abs(lastMagXList[i] - lastMagXList[j + i]) >= 12:
                            wasMotionPresent = True
            lastMagXList = []

        if l is not None and (abs(l[3]) == 0 or abs(l[4]) == 0):
            if lastRowTime == "":
                lastRowTime = l[0]
            TIMELAPSE = (l[0]-lastRowTime).total_seconds()
            if(TIMELAPSE >= 2):
                secondsSinceLastSignificantMovement += TIMELAPSE
                wasMissingGPS = True
                lastRowTime = l[0]

               #check for charging/magnetic data
                if int(l[26]) >= 10: #chose 10 instead of 0 to ignore potential noise
                    isCharging = True
                else:
                    isCharging = False
                if abs(l[10]) > 0:
                    magXDelta = abs(l[10] - lastMagX)
                    lastMagX = l[10]

                    if magXDelta > 12 and isCharging == False and secondsSinceLastSignificantMovement < 300:
                        if magXDelta > 15 and tripHasStarted == 1:
                            wasMotionPresent = True
                        if tripHasStarted == 0:
                            tripHasStarted = 1
                            tripEarlyStart = l[0]
                        #can't update distance, only time
                        tripEarlyEnd = l[0]
                        tripLateEnd = l[0]
                        tripEnd = l[0]
                        if magXDelta > 20:
                            secondsSinceLastSignificantMovement = 0
           
            if secondsSinceLastSignificantMovement >= 300 or isCharging == True:
                if tripHasStarted == 1: #did not move in last minute which we care about if we are on a trip
                    if 1000 < maxTripDist < 1000000 and wasMotionPresent: #low and high pass filter
                        if tripLateStart == 0:
                            tripLateStart = tripEarlyStart                            
                        tripStartTimes.append(tripStart)
                        tripEarliestStartTimes.append(tripEarlyStart)
                        tripLatestStartTimes.append(tripLateStart)
                        tripEndTimes.append(tripEnd)
                        tripEarliestEndTimes.append(tripEarlyEnd)
                        tripLatestEndTimes.append(tripLateEnd)
                        tripDists.append(tripDist/1000)
                        tripShortestDists.append(minTripDist/1000)
                        tripLongestDists.append(maxTripDist/1000)
                        tripShortestDurations.append((tripEarlyEnd - tripLateStart).total_seconds())
                        tripLongestDurations.append((tripLateEnd - tripEarlyStart).total_seconds())

                tripHasStarted = 0
                tripStart = 0
                tripEarlyStart = 0
                tripLateStart = 0
                tripEnd = 0
                tripEarlyEnd = 0
                tripLateEnd = 0
                secondsSinceLastSignificantMovement = 0
                tripDist = 0
                minTripDist = 0
                maxTripDist = 0
                wasMotionPresent = False

        if l is not None and abs(l[3]) > 1 and abs(l[4]) > 1:
            if lastRow == "" :
               lastRow = l
               lastRowTime = l[0]
               realLastRowTime = l[0]

            TIMELAPSE = (l[0]-lastRowTime).total_seconds()
            d = haversine(l[3],lastRow[3], l[4], lastRow[4])*1000
            if int(l[26]) >= 10: #chose 10 instead of 0 to ignore potential noise
                isCharging = True
            else:
                isCharging = False
            if l[10] != 0:
                magXDelta = abs(l[10] - lastMagX)
                lastMagX = l[10]
            if magXDelta > 15 and tripHasStarted == 1:
                wasMotionPresent = True
            
            if (TIMELAPSE >= 1200 or isCharging == True) and tripHasStarted == 1: #if we have more than a 10 minute gap in data, end prior trip if it exists   
               if 500 < maxTripDist < 1000000 and wasMotionPresent: #low and high pass filter
                   tripStartTimes.append(tripStart)
                   tripEarliestStartTimes.append(tripEarlyStart)
                   tripLatestStartTimes.append(tripLateStart)
                   tripEndTimes.append(tripEnd)
                   tripEarliestEndTimes.append(tripEarlyEnd)
                   tripLatestEndTimes.append(tripLateEnd)
                   tripDists.append(tripDist/1000)
                   tripShortestDists.append(minTripDist/1000)
                   tripLongestDists.append(maxTripDist/1000)
                   tripShortestDurations.append((tripEarlyEnd - tripLateStart).total_seconds())
                   tripLongestDurations.append((tripLateEnd - tripEarlyStart).total_seconds())

               tripHasStarted = 0
               tripStart = 0
               tripEarlyStart = 0
               tripLateStart = 0
               tripEnd = 0
               tripEarlyEnd = 0
               tripLateEnd = 0
               secondsSinceLastSignificantMovement = 0
               tripDist = 0  
               minTripDist = 0
               maxTripDist = 0
               lastRow = l
               lastRowTime = l[0]
               realLastRowTime = l[0]
               wasMotionPresent = False

            elif (TIMELAPSE >= 10): #only consider rows 10 seconds apart 
               REALTIMELAPSE = (l[0]-realLastRowTime).total_seconds()
               if d > 15 and 0 < d/(REALTIMELAPSE/3600) < 80000:
                   #assume that anything less than .5km/h or more than 80kmph is an error, and anything < 15 is an error
                   # start trip, or add to existing trip. 
                   if tripHasStarted == 0 or tripLateStart == 0:
                       tripHasStarted = 1
                       if wasMissingGPS == True:
                           tripLateStart = l[0]
                           tripStart = l[0]
                       else:
                           tripLateStart = lastRow[0]
                           tripStart = lastRow[0]
                       if tripEarlyStart == 0:
                           tripEarlyStart = tripLateStart
                   minTripDist += d
                   maxTripDist += d
                   tripDist += d
                   tripEnd = l[0]
                   tripEarlyEnd = l[0] #this will get updated if its a real trip
                   tripLateEnd = l[0]
                   if d > 100 or magXDelta > 20:
                       secondsSinceLastSignificantMovement = 0

               elif magXDelta > 12:
                   if tripHasStarted == 0 or tripEarlyStart == 0:
                       tripHasStarted = 1
                       if wasMissingGPS == True:
                           tripEarlyStart = l[0]
                       else:
                           tripEarlyStart = lastRow[0]
                       if tripLateStart != 0:
                           tripEarlyStart = tripLateStart
                   maxTripDist += d
                   tripLateEnd = l[0]
                   if magXDelta > 20:
                       secondSinceLastSignificantMovement = 0

               else:
                    if tripHasStarted == 1: #did not move in last minute which we care about if we are on a trip
                        secondsSinceLastSignificantMovement += TIMELAPSE
                        if secondsSinceLastSignificantMovement >= 300 or isCharging == True:
                            if 1000 < maxTripDist < 1000000 and wasMotionPresent == True: #low and high pass filter
                                tripStartTimes.append(tripStart)
                                tripEarliestStartTimes.append(tripEarlyStart)
                                tripLatestStartTimes.append(tripLateStart)
                                tripEndTimes.append(tripEnd)
                                tripEarliestEndTimes.append(tripEarlyEnd)
                                tripLatestEndTimes.append(tripLateEnd)
                                tripDists.append(tripDist/1000)
                                tripShortestDists.append(minTripDist/1000)
                                tripLongestDists.append(maxTripDist/1000)
                                tripShortestDurations.append((tripEarlyEnd - tripLateStart).total_seconds())
                                tripLongestDurations.append((tripLateEnd - tripEarlyStart).total_seconds())

                            tripHasStarted = 0
                            tripStart = 0
                            tripEarlyStart = 0
                            tripLateStart = 0
                            tripEnd = 0
                            tripEarlyEnd = 0
                            tripLateEnd = 0
                            secondsSinceLastSignificantMovement = 0
                            tripDist = 0
                            minTripDist = 0
                            maxTripDist = 0
                            wasMotionPresent = False
               lastRow = l
               lastRowTime = l[0]
               realLastRowTime = l[0]
               wasMissingGPS = False
    # don't return non-interval times
    return tripEarliestStartTimes, tripLatestStartTimes, tripEarliestEndTimes, tripLatestEndTimes, tripShortestDurations, tripLongestDurations, tripShortestDists, tripLongestDists
    


def detectChargingEvents(dbc,imei,startDate,endDate):
    
    stmt = "select stamp, ChargingCurr, batteryvoltage from imei{0} where stamp >= \"{1}\" and stamp <= \"{2}\" and chargingCurr is not null order by stamp;".format(imei, startDate, endDate)
    lastRow = ""
    lastRowTime = ""
    
    chargingHasStarted = 0
    chargingStart = 0
    chargingEnd = 0
    chargingStartVoltage = 0
    chargingEndVoltage = 0
    secondsSinceLastSignifigantMovement = 0
    #tripDist = 0
    
    
    chargeStartTimes = []
    chargeEndTimes = []
    chargeStartVolts = []
    chargeEndVolts= []
    
    
    for l in dbc.SQLSelectGenerator(stmt):
        if l is not None:
            #print(l)
            if lastRow == "" :
               lastRow = l
               lastRowTime = l[0]
                
            TIMELAPSE = (l[0]-lastRowTime).total_seconds()
            
            if TIMELAPSE >= 600 and chargingHasStarted == 1: #if we have more than a 10 minute gap in data, end prior charging event if it exists   
               if 60 <  (chargingEnd-chargingStart).total_seconds():
                  chargeStartTimes.append(chargingStart)
                  chargeEndTimes.append(chargingEnd)
                  chargeStartVolts.append(chargingStartVoltage)
                  chargeEndVolts.append(chargingEndVoltage)
               chargingHasStarted = 0
               chargingStart = 0
               chargingEnd = 0
               chargingStartVoltage = 0
               chargingEndVoltage = 0
               secondsSinceLastSignifigantMovement = 0
               lastRow = l
               lastRowTime = l[0]

            elif (TIMELAPSE >= 5): #only consider rows 10 seconds apart 
               #print(l[1]) 
               if l[1] > 20: 
                   if chargingHasStarted == 0:
                       chargingHasStarted = 1
                       chargingStart = l[0]
                       chargingStartVoltage = l[2]
                   chargingEnd = l[0] #this will get updated if its a real trip
                   chargingEndVoltage = l[2]

               else:
                    if chargingHasStarted == 1: #did not move in last minute which we care about if we are on a trip
                        #print("TRIGGERING2")
                        secondsSinceLastSignifigantMovement += TIMELAPSE
                        if chargingHasStarted >= 300:
                            if 60 <  (chargingEnd-chargingStart).total_seconds():
                               chargeStartTimes.append(chargingStart)
                               chargeEndTimes.append(chargingEnd)
                               chargeStartVolts.append(chargingStartVoltage)
                               chargeEndVolts.append(chargingEndVoltage)
                            chargingHasStarted = 0
                            chargingStart = 0
                            chargingEnd = 0
                            chargingStartVoltage = 0
                            chargingEndVoltage = 0
                            secondsSinceLastSignifigantMovement = 0
               lastRow = l
               lastRowTime = l[0]
    return chargeStartTimes, chargeEndTimes, chargeStartVolts, chargeEndVolts

