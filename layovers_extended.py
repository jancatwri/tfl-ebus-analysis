from __future__ import print_function
import pandas as pd
import numpy as np
import glob, os, shutil
import gzip
import json
import base64
import datetime
import pprint
import uuid
import sys
import numpy as np

from dateutil import parser, rrule
parsedate = parser.parse
to_pytimedelta = pd.Timedelta.to_pytimedelta

def layoveranalysis(oneday,n,YYYY,MM,DD,name,batteryMax,chargePower,discharge,heating,heatingBool,dwellloss):
    initialisation = pd.DataFrame({'Initialisation Values':[batteryMax,chargePower,discharge,heating,heatingBool,dwellloss]},
                                   index = ['Battery Capacity (kWh)','Charger Power (kW)','Traction Energy requirement (kWh/km)','Heating Power (kW)','Heating in Calculation?','Dwell Time Loss (min)'])


    # In[5]:


    if oneday == True:
        results = pd.read_csv('./Bus_{}8/{}-{}-{}/combinedfile.csv'.format(n,YYYY,MM,DD))
    else:
        results = pd.read_csv('./{}combinedfile.csv'.format(name))


    # In[6]:


    results_ = results.drop(columns={'bearing','Unnamed: 0','Unnamed: 0.1','$type','id','currentLocation','destinationNaptanId','timing','modeName','towards','lineName','platformName','operationType','timeToLive','timeToStation'}).reset_index(drop=True)
    results_ = results_.sort_values(by=['vehicleId','naptanId','expectedArrival'], ascending=False).reset_index(drop=True)


    # In[7]:


    if oneday == True:
        results_.to_csv('./Bus_{}8/{}-{}-{}/Data.csv'.format(n,YYYY,MM,DD))
    else:
        results_.to_csv('./{}_Data.csv'.format(name))


    # In[8]:


    parsed=pd.DataFrame
    parsed=results_
    parsed.insert(3,'expectedArrivalParse','')
    parsed.insert(4,'stop','')


    # In[9]:


    parsed['expectedArrivalParse'] = [parsedate(x) for x in parsed['expectedArrival']]


    # In[10]:


    def gap_check(p2, p1):
        delta = p2['expectedArrivalParse'] - p1['expectedArrivalParse']
        delta_seconds = delta.total_seconds()
        return delta_seconds < 30*60


    # In[11]:


    rows = parsed.shape[0]
    group = [rows]
    group = [1]


    # In[12]:


    key = 1
    for x in range(1,(rows)): 
        if gap_check(parsed.iloc[x-1], parsed.iloc[x]) and (parsed['vehicleId'].iloc[x-1]==parsed['vehicleId'].iloc[x]) and (parsed['naptanId'].iloc[x-1]==parsed['naptanId'].iloc[x]):
            group.append(key)
        else:
            key=key+1
            group.append(key)


    # In[13]:


    group_ = pd.Series(group)
    parsed['stop'] = group_


    # In[211]:


    condensed = pd.DataFrame
    condensed = parsed.sort_values(by=['vehicleId','naptanId','stop','timestamp'],ascending=False).reset_index(drop=True)
    condensed = condensed.drop_duplicates(['stop'], keep='first').reset_index(drop=True)
    condensed = condensed.sort_values(by=['vehicleId','expectedArrival']).reset_index(drop=True)
    condensed = condensed.drop(columns=['stop','timestamp'])


    # In[212]:


    if oneday == True:
        condensed.to_csv('./Bus_{}8/{}-{}-{}/Data_noduplicates.csv'.format(n,YYYY,MM,DD))
    else:
        condensed.to_csv('./{}_Data_noduplicates.csv'.format(name))


    # In[213]:


    def tripdivisionstart(condensed):
        check = (condensed['direction'].iloc[x-1]!=condensed['direction'].iloc[x])             or (condensed['vehicleId'].iloc[x-1]!=condensed['vehicleId'].iloc[x])             or ((condensed['expectedArrivalParse'].iloc[x] - condensed['expectedArrivalParse'].iloc[x-1]).total_seconds()>60*30)
        return check


    # In[214]:


    def tripdivisionend(condensed):
        check = (condensed['direction'].iloc[x+1]!=condensed['direction'].iloc[x])             or (condensed['vehicleId'].iloc[x+1]!=condensed['vehicleId'].iloc[x])             or ((condensed['expectedArrivalParse'].iloc[x+1] - condensed['expectedArrivalParse'].iloc[x]).total_seconds()>60*30)
        return check


    # In[215]:


    rows = condensed.shape[0]
    group = [rows]
    group = [1]
    check = 0
    for x in range(1,(rows-1)):
            if tripdivisionstart(condensed):
                if check ==1:
                    group = group[:-1]
                    group.append('')
                group.append(1)
                check=1
            elif tripdivisionend(condensed):
                if check ==2:
                    group = group[:-1]
                    group.append('')
                group.append(2)
                check=2
            else:
                group.append('')

    group.append(2)
    group_ = pd.Series(group)
    condensed['trip'] = group_


    # In[216]:


    condensed['trip'] = condensed['trip'].replace('', np.nan)
    condensed.dropna(subset=['trip'], inplace=True)
    condensed = condensed.reset_index(drop=True)


    # In[217]:


    check=0
    for x in range(0,condensed.shape[0]-2):
        if condensed['destinationName'].iloc[x]==condensed['destinationName'].iloc[x+1]:
            check=check+1
        else:
            if check == 2 or 0:
                print('Error establishing start and end points')
            check=0


    # In[218]:


    if oneday == True:
        condensed.to_csv('./Bus_{}8/{}-{}-{}/Data_firstlast.csv'.format(n,YYYY,MM,DD))
    else:
        condensed.to_csv('./{}_Data_firstlast.csv'.format(name))


    # In[219]:


    allStops = pd.read_csv('./LondonNaptanCodes.csv')
    allStops = pd.DataFrame({'naptanId':allStops['ATCOCode'],'Latitude':allStops['Latitude'],'Longitude':allStops['Longitude']})


    # In[220]:


    condensed = pd.merge(condensed,allStops,how='left')


    # In[221]:


    links = pd.DataFrame(columns=['destinationName','start','end','linkName','startLat','startLong','endLat','endLong'])
    destination =[]
    link=[]
    startLat=[]
    startLong=[]
    endLat=[]
    endLong=[]
    start=[]
    end=[]
    for x in range (0,condensed.shape[0],2):
        destination.append(condensed['destinationName'].iloc[x])
        start.append(condensed['stationName'].iloc[x])
        end.append(condensed['stationName'].iloc[x+1])
        link.append(condensed['stationName'].iloc[x] + ' to ' + condensed['stationName'].iloc[x+1])
        startLat.append(condensed['Latitude'].iloc[x])
        startLong.append(condensed['Longitude'].iloc[x])
        endLat.append(condensed['Latitude'].iloc[x+1])
        endLong.append(condensed['Longitude'].iloc[x+1])

    links['destinationName']=destination
    links['start']=start
    links['end']=end
    links['linkName']=link
    links['startLat']=startLat
    links['endLat']=endLat
    links['startLong']=startLong
    links['endLong']=endLong

    links = links.drop_duplicates(['linkName'], keep='first')
    links = links.sort_values(by=['destinationName','linkName']).reset_index(drop=True)
    links.head()


    # In[223]:


    destinations = list(np.unique(links['destinationName']))
    linklist = pd.DataFrame(columns=destinations)
    linklist = linklist.to_dict()
    for dest in destinations:
        linklist[dest]= list(np.unique(links['linkName'].loc[links['destinationName'] == dest]))


    # In[224]:


    condensed.insert(3,'expectedArrival_','')
    condensed['expectedArrival_']=pd.to_datetime(condensed['expectedArrival'])


    # In[62]:


    firstlast = condensed.copy()
    firstlast.insert(9,'layoverTime','-')
    firstlast.insert(10,'journeyTime','-')
    firstlast.insert(11,'eitherTime','-')


    # In[63]:


    def time(p2, p1):
        delta = parsedate(p2) - parsedate(p1)
    #    delta_seconds = delta.total_seconds()
        return delta


    # In[64]:


    eithertime =[]
    journeytime=['']
    layovertime=['']
    eithertime.append(firstlast['expectedArrival_'].iloc[0])
    for x in range(1,(firstlast.shape[0])):
        if (firstlast['vehicleId'].iloc[x] == firstlast['vehicleId'].iloc[x-1]):
            if (firstlast['trip'].iloc[x] == 2):
                journeytime.append(time(firstlast['expectedArrival'].iloc[x],firstlast['expectedArrival'].iloc[x-1]))
                eithertime.append(journeytime[-1])
                layovertime.append('')
            else:
                layovertime.append(time(firstlast['expectedArrival'].iloc[x],firstlast['expectedArrival'].iloc[x-1]))
                eithertime.append(layovertime[-1])
                journeytime.append('')
        else:
            eithertime.append(firstlast['expectedArrival_'].iloc[x])
            journeytime.append('')
            layovertime.append('')

    firstlast['journeyTime'] = journeytime
    firstlast['layoverTime'] = layovertime
    firstlast['eitherTime'] = eithertime

    firstlast


    # In[65]:


    if oneday == True:
        firstlast.to_csv('./Bus_{}8/{}-{}-{}/Data_times.csv'.format(n,YYYY,MM,DD))
    else:
        firstlast.to_csv('./{}_Data_times.csv'.format(name))


    # In[66]:


    vehiclelist = np.unique(firstlast['vehicleId'])
    len(vehiclelist)


    # In[67]:


    count = pd.DataFrame
    count = firstlast.groupby('vehicleId')['vehicleId'].count()
    maxi = max(count)


    # In[68]:


    layover = 'Layover'


    # In[69]:


    def space():
        times.append('')
        return


    # In[70]:


    times_names = ['Start']


    # In[71]:


    for i in range(maxi):
        for dest in destinations:
            times_names.extend([layover])
            times_names.extend(linklist[dest])


    # In[72]:


    times_= pd.DataFrame(times_names,columns=['instance'])


    # In[73]:


    for veh in vehiclelist:
        times = []
        x = 0
        for index,row in firstlast.iterrows():
            if (firstlast['vehicleId'].iloc[index]==veh):
                if not times:
                    times.append(firstlast['eitherTime'].iloc[index])
                    x=x+1
                else:
                    y=0
                    while True:
                        if times_['instance'].iloc[x]==layover:
                            if (firstlast['trip'].iloc[index]==1):
                                times.append(to_pytimedelta(firstlast['layoverTime'].iloc[index]))
                                x=x+1
                                y=1
                            else:
                                space()
                                x=x+1
                        else:
                            if (firstlast['trip'].iloc[index]==2):
                                if bool((str(firstlast['stationName'].iloc[index-1]+' to') in (times_['instance'].iloc[x])) and (firstlast['stationName'].iloc[index] in (times_['instance'].iloc[x]))):
                                    times.append(to_pytimedelta(firstlast['journeyTime'].iloc[index]))
                                    x=x+1
                                    y=1
                                else:
                                    space()
                                    x=x+1  
                            else:
                                space()
                                x=x+1
                        if y > 0:
                            break

        if len(times) < len(times_):
            for i in range(0,(len(times_)-len(times))):
                space()

        times_[veh]=times

    times_=times_.rename(columns={'instance':'start/link/layover'})
    times_=times_.sort_values(by=[0,1], axis=1, ascending=False)
    times_=times_.replace('', np.NaN)
    times_=times_.dropna(axis=0,thresh=2)
    times_=times_.fillna('-')


    # In[74]:


    if oneday == True:
        times_.to_csv('./Bus_{}8/{}-{}-{}/Data_busgraph.csv'.format(n,YYYY,MM,DD),index=False)
    else:
        times_.to_csv('./{}_Data_busgraph.csv'.format(name),index=False)

    print('Bus Graph csv Complete')
    # ## Battery
    
    print('Battery Started')

    # In[281]:


    import collections
    from datetime import datetime
    from datetime import timedelta
    import functools
    import hashlib
    import hmac
    import re
    import requests
    import random
    import time
    import googlemaps

    def distanceapi(origin,destination):
    #    origin = origin + ', London'
    #    destination = destination + ', London'
        key = 'AIzaSyBf21cgECqW0-6-1yPp4_PyvkKSarpya2c'
        mode = "driving"
        units = "metric"
        region = "uk"
        request = googlemaps.Client(key)

        result = googlemaps.distance_matrix.distance_matrix(request,origin,destination)
        km = result['rows'][0]['elements'][0]['distance']['text']
        if km[-2]=='k':
            km = km[:-3]
        else:
            km = float(km[:-2])/1000
        return km


    # In[282]:


    distances =[]
    for index,row in links.iterrows():
        origin = '{},{}'.format(links['startLat'].iloc[index].astype(str), links['startLong'].iloc[index].astype(str))
        destination = '{},{}'.format(links['endLat'].iloc[index].astype(str),links['endLong'].iloc[index].astype(str))
        distances.append(distanceapi(origin,destination))


    # In[289]:


    distances = [float(i) for i in distances]
    links['distance (km)']=distances
    links = links.sort_values(['distance (km)'],ascending=False).reset_index(drop=True)


    # In[119]:


    rows=firstlast.shape[0]
    time=[]
    battery = []
    batt = batteryMax
    batt2 = 0.0
    battdif = 0.0
    batttime= 0.0
    dwelltime = 0
    batteryTotal = []
    vehicle=[]


    # In[120]:


    startlist = []
    endlist =  []
    foundlinkindex = []
    linkdistance = 0.0
    for index,row in firstlast.iterrows():
        veh = firstlast['vehicleId'][index]
        if (pd.isnull(firstlast['layoverTime'].iloc[index]) and pd.isnull(firstlast['journeyTime'].iloc[index])):
            batt = batteryMax
            battery.append(batt)
            time.append(firstlast['eitherTime'][index])
            vehicle.append(veh)
            batteryTotal.append(0.0)
        elif (pd.isnull(firstlast['layoverTime'].iloc[index])):
            startlist = [i for i, e in enumerate(links['start']) if e == firstlast['stationName'][index-1]]
            endlist = [i for i, e in enumerate(links['end']) if e == firstlast['stationName'][index]]
            foundlinkindex = [x for x in startlist if x in set(endlist)]
            linkdistance = links['distance (km)'].iloc[foundlinkindex[0]]
            batt = batt-(linkdistance*discharge)
            if heatingBool == True:
                batt = batt - firstlast['journeyTime'][index].total_seconds()*heating/3600
            battery.append(batt)
            time.append(firstlast['expectedArrival'][index])
            vehicle.append(veh)
            batteryTotal.append(linkdistance*discharge + (heatingBool*firstlast['journeyTime'][index].total_seconds()*heating/3600))
        elif (pd.isnull(firstlast['journeyTime'].iloc[index])):
            dwelltime = (firstlast['layoverTime'][index]).total_seconds() - dwellloss*60
            if dwelltime <0:
                dwelltime = 0
            batt2 = batt + (dwelltime*chargePower/3600)
            if batt2 > batteryMax:
                battdif = (batteryMax - batt)*3600
                batttime = battdif/chargePower
                batttime = timedelta(seconds=batttime)
                batttime = batttime + parsedate(firstlast['expectedArrival'][index-1])
                battery.append(batteryMax)
                time.append(batttime.isoformat())
                vehicle.append(veh)
                batteryTotal.append(0.0)
                batt = batteryMax
                battery.append(batt)
                time.append(firstlast['expectedArrival'][index])
                vehicle.append(veh)
                batteryTotal.append(0.0)
            else:
                batt=batt2
                battery.append(batt2)
                time.append(firstlast['expectedArrival'][index])
                vehicle.append(veh)
                batteryTotal.append(0.0)


    # In[121]:


    time_=pd.Series(time)
    battery_ = pd.Series(battery)
    vehicle_=pd.Series(vehicle)

    data1 = {'time':time_,'vehicleId':vehicle_,'batterykWh':battery_,}
    batterycalc=pd.DataFrame(data1)


    # In[122]:


    batteryTotal_=pd.Series(batteryTotal)

    data2 = {'vehicleId':vehicle_,'totalDischargekWh':batteryTotal_,}
    batterydischarge = pd.DataFrame(data2)
    batterydischarge = batterydischarge.groupby(['vehicleId']).sum()
    batterydischarge = batterydischarge.sort_values(['totalDischargekWh'],ascending=False)


    # In[123]:


    def timedeltaformat(time):
        time = time.total_seconds()
        hours, remainder = divmod(time, 3600)
        minutes, seconds = divmod(remainder, 60)
        hours = int(hours)
        minutes = int(minutes)
        duration_formatted = '%02d:%02d:%02d' % (hours, minutes, seconds)
        return duration_formatted


    # In[124]:


    batterylayover = pd.DataFrame({'vehicleId':firstlast['vehicleId'],'dwellTotal':firstlast['layoverTime']})
    batterylayover = batterylayover.dropna(axis=0,thresh=2)
    batterylayover = batterylayover.groupby(['vehicleId']).sum()
    batterylayover['dwellTotal'] = [timedeltaformat(x) for x in batterylayover['dwellTotal']]
    batterylayover = batterylayover.sort_values(['dwellTotal'],ascending=False)


    # In[125]:


    batterycalc_= pd.DataFrame(batterycalc['time'],columns=['time'])


    # In[126]:


    for veh in vehiclelist:
        batterykWh = []
        for index,row in batterycalc.iterrows():
            if (batterycalc['vehicleId'].iloc[index]==veh):
                batterykWh.append(batterycalc['batterykWh'][index])
            else:
                batterykWh.append('')
        batterycalc_[veh]=batterykWh


    # In[127]:


    error_veh=[]
    error_time=[]
    error_kWh=[]

    batteryLimit = 25

    for index,row in batterycalc.iterrows():
        if (batterycalc['batterykWh'].iloc[index]<(batteryMax*batteryLimit/100)):
            error_veh.append(batterycalc['vehicleId'].iloc[index])
            error_time.append(batterycalc['time'].iloc[index])
            error_kWh.append(batterycalc['batterykWh'].iloc[index])

    if len(error_time)==0:
        errors =errors=pd.DataFrame(['No journeys reaching {}% SOC'.format(batteryLimit)])
        worstcasesbattery=pd.DataFrame(['No journeys reaching {}% SOC'.format(batteryLimit)])
        worstcasesgraph=pd.DataFrame(['No journeys reaching {}% SOC'.format(batteryLimit)])
    else:
        errors=pd.DataFrame(columns=['time','vehicleId','batterykWh'])
        errors['time']=error_time
        errors['vehicleId']=error_veh
        errors['batterykWh']=error_kWh
        error_veh = np.unique(error_veh)

        worstcasesbattery=pd.DataFrame(batterycalc_[error_veh],columns=error_veh)
        worstcasesbattery.insert(0,'time',batterycalc_['time'])
        worstcasesbattery=worstcasesbattery.replace('', np.NaN)
        worstcasesbattery=worstcasesbattery.dropna(axis=0,thresh=2)

        worstcasesgraph=pd.DataFrame(times_[error_veh],columns=error_veh)
        worstcasesgraph.insert(0,'start/link/layover',times_['start/link/layover'])
        worstcasesgraph=worstcasesgraph.replace('-', np.NaN)
        worstcasesgraph=worstcasesgraph.dropna(axis=0,thresh=2)
    errors


    # In[128]:


    if oneday == True:
        writer = pd.ExcelWriter('./Bus_{}8/{}-{}-{}/Data_battery_heating{}.xlsx'.format(n,YYYY,MM,DD,heatingBool))
        batterycalc_.to_excel(writer,'Sawtooth Data',index=False)
        errors.to_excel(writer,'SOC below 25%')
        batterydischarge.to_excel(writer,'Total Discharge')
        writer.save()
        writer.close()
    else:
        writer = pd.ExcelWriter('./{}_Data_battery_heating{}.xlsx'.format(name,heatingBool))
        initialisation.to_excel(writer,'Initialisation Values')
        batterycalc_.to_excel(writer,'Sawtooth Data',index=False)
        errors.to_excel(writer,'{}% SOC instances'.format(batteryLimit))
        worstcasesbattery.to_excel(writer,'Sawtooth Data (<{}% SOC)'.format(batteryLimit),index=False)
        worstcasesgraph.to_excel(writer,'Bus Graph Data (<{}% SOC)'.format(batteryLimit),index=False)
        batterydischarge.to_excel(writer,'Total Discharge')
        batterylayover.to_excel(writer,'Total Dwell Time')
        writer.save()
    
    print('Battery Complete')

