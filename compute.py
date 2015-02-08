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
    tripStart = 0
    tripEnd = 0
    secondsSinceLastSignifigantMovement = 0
    tripDist = 0
    
    tripStartTimes = []
    tripEndTimes = []
    tripDists = []
    
    for l in dbc.SQLSelectGenerator(stmt):
        if l is not None and abs(l[3]) > 1 and abs(l[4]) > 1:
            #print(l)
            if lastRow == "" :
               lastRow = l
               lastRowTime = l[0]
                
            TIMELAPSE = (l[0]-lastRowTime).total_seconds()
            d = haversine(l[3],lastRow[3], l[4], lastRow[4])*1000
            
            if TIMELAPSE >= 1200 and tripHasStarted == 1: #if we have more than a 10 minute gap in data, end prior trip if it exists   
               if 500 < tripDist < 1000000: #low and high pass filter
                   tripStartTimes.append(tripStart)
                   tripEndTimes.append(tripEnd)
                   tripDists.append(tripDist/1000)
               tripHasStarted = 0
               tripStartTime = 0
               tripEndTime = 0
               secondsSinceLastSignifigantMovement = 0
               tripDist = 0  
               lastRow = l
               lastRowTime = l[0]

            elif (TIMELAPSE >= 10): #only consider rows 10 seconds apart 
               if d > 15 and 1000 < d/(TIMELAPSE/3600) < 80000: 
                   #assume that anything less than .5km/h or more than 80kmph is an error, and anything < 15 is an error
                   # start trip, or add to existing trip. 
                   if tripHasStarted == 0:
                       tripHasStarted = 1
                       #print("STARTING TRIP")
                       tripStart = l[0]
                   tripDist += d    
                   tripEnd = l[0] #this will get updated if its a real trip

               else:
                    if tripHasStarted == 1: #did not move in last minute which we care about if we are on a trip
                        #print("TRIGGERING2")
                        secondsSinceLastSignifigantMovement += TIMELAPSE
                        if secondsSinceLastSignifigantMovement >= 300:
                            if 1000 < tripDist < 1000000: #low and high pass filter
                                tripStartTimes.append(tripStart)
                                tripEndTimes.append(tripEnd)
                                tripDists.append(tripDist/1000)
                            tripHasStarted = 0
                            tripStartTime = 0
                            tripEndTime = 0
                            secondsSinceLastSignifigantMovement = 0
                            tripDist = 0
               lastRow = l
               lastRowTime = l[0]
    print(len(tripStartTimes))
    return tripStartTimes, tripEndTimes, tripDists
    


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






       
       
