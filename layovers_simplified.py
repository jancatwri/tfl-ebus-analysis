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

def layoveranalysis(n,YYYY,MM,DD,first_stops):
    # In[464]:


    results = pd.read_csv('./Bus_{}8/{}-{}-{}/combinedfile.csv'.format(n,YYYY,MM,DD))


    # In[465]:


    results_ = results.drop(columns={'bearing','Unnamed: 0','Unnamed: 0.1','$type','id','currentLocation','destinationNaptanId','timing','modeName','towards','lineName','platformName','operationType','timeToLive','timeToStation'}).reset_index(drop=True)
    results_ = results_.sort_values(by=['vehicleId','naptanId','expectedArrival'], ascending=False).reset_index(drop=True)


    # In[466]:


    results_.to_csv('./Bus_{}8/{}-{}-{}/Data.csv'.format(n,YYYY,MM,DD))


    # In[467]:


    parsed=pd.DataFrame
    parsed=results_
    parsed.insert(3,'expectedArrivalParse','')
    parsed.insert(4,'stop','')


    # In[468]:


    parsed['expectedArrivalParse'] = [parsedate(x) for x in parsed['expectedArrival']]


    # In[469]:


    def gap_check(p2, p1):
        delta = p2['expectedArrivalParse'] - p1['expectedArrivalParse']
        delta_seconds = delta.total_seconds()
        return delta_seconds < 40*60


    # In[470]:


    rows = parsed.shape[0]
    group = [rows]
    group = [1]


    # In[471]:


    key = 1
    for x in range(1,(rows)): 
        if gap_check(parsed.iloc[x-1], parsed.iloc[x]) and (parsed['vehicleId'].iloc[x-1]==parsed['vehicleId'].iloc[x]) and (parsed['naptanId'].iloc[x-1]==parsed['naptanId'].iloc[x]):
            group.append(key)
        else:
            key=key+1
            group.append(key)


    # In[472]:


    group_ = pd.Series(group)
    parsed['stop'] = group_

    # In[473]:


    condensed = pd.DataFrame
    condensed = parsed.sort_values(by=['vehicleId','naptanId','stop','timestamp'],ascending=False).reset_index(drop=True)
    condensed = condensed.drop_duplicates(['stop'], keep='first').reset_index(drop=True)
    condensed = condensed.sort_values(by=['vehicleId','expectedArrival']).reset_index(drop=True)
    condensed = condensed.drop(columns=['stop','timestamp'])
    condensed.to_csv('./Bus_{}8/{}-{}-{}/Data_noduplicates.csv'.format(n,YYYY,MM,DD))


    # In[474]:


    rows = condensed.shape[0]
    group = [rows]
    group = [1]
    for x in range(1,(rows-1)):
            if (condensed['direction'].iloc[x+1]!=condensed['direction'].iloc[x]) or (condensed['direction'].iloc[x-1]!=condensed['direction'].iloc[x]) or (condensed['vehicleId'].iloc[x-1]!=condensed['vehicleId'].iloc[x]) or (condensed['vehicleId'].iloc[x+1]!=condensed['vehicleId'].iloc[x]):# or ((condensed['expectedArrivalParse'].iloc[x+1] - condensed['expectedArrivalParse'].iloc[x]).total_seconds()>60*15) or ((condensed['expectedArrivalParse'].iloc[x] - condensed['expectedArrivalParse'].iloc[x-1]).total_seconds()>60*10):
                group.append(1)
            else:
                group.append('')

    group.append(1)
    group_ = pd.Series(group)
    condensed['trip'] = group_


    # In[475]:


    condensed['trip'] = condensed['trip'].replace('', np.nan)
    condensed.dropna(subset=['trip'], inplace=True)
    condensed = condensed.reset_index(drop=True)

    # In[476]:


    condensed.to_csv('./Bus_{}8/{}-{}-{}/Data_firstlast.csv'.format(n,YYYY,MM,DD))


    # In[477]:


    rows = condensed.shape[0]
    group = [rows]
    check = 0
    if condensed['direction'].iloc[0]=='outbound':
        key=0
        group = [first_stops[key]]
    else:
        key=1
        group = [first_stops[key]]

    for x in range(1,(rows)):
        if (condensed['vehicleId'].iloc[x-1]==condensed['vehicleId'].iloc[x]):
            if (condensed['direction'].iloc[x-1]==condensed['direction'].iloc[x]):
                key =(key + 1) % 2
                group.append(first_stops[key])
            else:
                group.append(first_stops[key])
        else:
            if condensed['direction'].iloc[x]=='outbound':
                key=0
                group.append(first_stops[key])
            else:
                key=1
                group.append(first_stops[key])        

    group_ = pd.Series(group)
    condensed['station'] = group_
    condensed = condensed.drop(columns=['trip'])


    # In[478]:


    condensed.insert(3,'expectedArrival_','')
    condensed['expectedArrival_']=pd.to_datetime(condensed['expectedArrival'])


    # In[480]:


    condensed.to_csv('./Bus_{}8/{}-{}-{}/Data_predictions.csv'.format(n,YYYY,MM,DD))


    # In[481]:


    firstlast = pd.DataFrame(condensed)
    firstlast.insert(9,'layoverTime','-')
    firstlast.insert(10,'journeyTime','-')
    firstlast.insert(11,'eitherTime','-')

    # In[482]:


    def time(p2, p1):
        delta = parsedate(p2) - parsedate(p1)
    #    delta_seconds = delta.total_seconds()
        return delta


    # In[483]:


    firstlast['eitherTime'].iloc[0] = firstlast['expectedArrival_'].iloc[0]
    for x in range(1,(firstlast.shape[0])):
        if firstlast['vehicleId'].iloc[x] == firstlast['vehicleId'].iloc[x-1]:
            if firstlast['station'].iloc[x-1] == firstlast['station'].iloc[x]:
                firstlast['layoverTime'].iloc[x]=time(firstlast['expectedArrival'].iloc[x],firstlast['expectedArrival'].iloc[x-1])
                firstlast['eitherTime'].iloc[x]=firstlast['layoverTime'].iloc[x]
            else:
                firstlast['journeyTime'].iloc[x]=time(firstlast['expectedArrival'].iloc[x],firstlast['expectedArrival'].iloc[x-1])
                firstlast['eitherTime'].iloc[x]=firstlast['journeyTime'].iloc[x]
        else:
            firstlast['eitherTime'].iloc[x]=firstlast['expectedArrival_'].iloc[x]

    # In[484]:


    firstlast.to_csv('./Bus_{}8/{}-{}-{}/Data_times.csv'.format(n,YYYY,MM,DD))


    # In[485]:


    vehiclelist = np.unique(firstlast['vehicleId'])
    len(vehiclelist)


    # In[486]:


    count = pd.DataFrame
    count = firstlast.groupby('vehicleId')['vehicleId'].count()
    maxi = max(count)


    # In[487]:


    outbound = first_stops[0] + ' to ' + first_stops[1]
    inbound = first_stops[1] + ' to ' + first_stops[0]
    layover = 'Layover'


    # In[488]:


    def space():
        times.append('-')
        return


    # In[489]:


    times_names = [first_stops[0],first_stops[1]]
    for i in range(maxi*2):
        times_names.extend([layover])
        times_names.extend([inbound])
        times_names.extend([outbound])


    # In[490]:


    times_= pd.DataFrame(times_names,columns=['instance'])


    # In[491]:


    for veh in vehiclelist:
        times = []
        x = 0
        for index,row in firstlast.iterrows():
            if (firstlast['vehicleId'].iloc[index]==veh):
                if not times:
                    if (firstlast['station'].iloc[index]==first_stops[0]):
                        times.append(firstlast['eitherTime'].iloc[index])
                        space()
                        x=x+2
                    else:
                        space()
                        times.append(firstlast['eitherTime'].iloc[index])
                        x=x+2
                else:
                    y=0
                    while True:
                        if times_['instance'].iloc[x]==layover:
                            if (firstlast['layoverTime'].iloc[index]!='-'):
                                times.append(firstlast['layoverTime'].iloc[index])
                                x=x+1
                                y=y+1
                            else:
                                space()
                                x=x+1
                        elif times_['instance'].iloc[x]==inbound:
                            if (firstlast['journeyTime'].iloc[index]!='-') and (firstlast['station'].iloc[index]==first_stops[0]):
                                times.append(firstlast['journeyTime'].iloc[index])
                                x=x+1
                                y=y+1
                            else:
                                space()
                                x=x+1
                        elif times_['instance'].iloc[x]==outbound:
                            if (firstlast['journeyTime'].iloc[index]!='-') and (firstlast['station'].iloc[index]==first_stops[1]):
                                times.append(firstlast['journeyTime'].iloc[index])
                                x=x+1
                                y=y+1
                            else:
                                space()
                                x=x+1
                        if y > 0:
                            break

        if len(times) < len(times_):
            for i in range(0,(len(times_)-len(times))):
                times.append('-')

        times_[veh]=times

    times_=times_.rename(columns={'instance':''})


    # In[492]:


    times_=times_.sort_values(by=[0,1], axis=1, ascending=True)

    # In[493]:


    times_.to_csv('./Bus_{}8/{}-{}-{}/Data_busgraph.csv'.format(n,YYYY,MM,DD))