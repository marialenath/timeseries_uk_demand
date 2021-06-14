# -*- coding: utf-8 -*-


# Libraries
from datetime import datetime,timedelta
import requests
import re
import pyodbc 
import pytz
import calendar

local = pytz.timezone ("Europe/London")


api_key = ''

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')

def get_raw_data(StartDate, StopDate):
   
    # Data
    print('Getting {0} to {1}'.format(StartDate, StopDate))
    link = f'https://api.bmreports.com/BMRS/INDOITSDO/V1?APIKey={api_key}&FromDate={StartDate}&ToDate={StopDate}&ServiceType=CSV'
    thedata = requests.get(link)
    return thedata

def process_raw_data (thedata):

    lst=[]

    for row in thedata.iter_lines():
        sp_date = re.findall('INDO,([0-9]*?),[0-9]*?,', str(row))
        try:
            sp_date = sp_date[0]
        except:
            continue


        sp = re.findall('INDO,[0-9]*?,([0-9]*?),', str(row))
        try:
            sp = sp[0]
        except:
            continue


        load_act = re.findall("INDO,[0-9]*?,[0-9]*?,[A-Za-z]*?,[0-9]*?,([0-9]*?)'", str(row))
        try:
            load_act = load_act[0]
            # replace zero value with missing
            if load_act == '0':
                load_act = 'NULL'
        except:
            load_act = 'NULL'


        lst.append([sp_date,sp, load_act])

    return lst



def update_data (result, conn):
    cursor = conn.cursor()
    for index,row in result.iterrows():
        
        cursor.execute("""INSERT INTO dissertation.dbo.bm_load(
                                [sp_date], [sp],
                                [load_act])
                                values (?, ?, ?)""", 
                               row['sp_date'], row['sp'], 
                               row['load_act'])
        conn.commit()
        
        

def update_gmt_data (result, conn):
    cursor = conn.cursor()
    for index,row in result.iterrows():
        
        cursor.execute("""INSERT INTO dissertation.dbo.bm_gmt_load(
                                [datetime_gmt], [datetime_local],[day],
                                [month], [year], [hour], [minute],
                                [load_act])
                                values (?, ?, ?, ? , ?, ?, ?, ?)""", 
                               row['datetime'], row['sp_datetime'], 
                               row['day'], row['month'], row['year'], row['hour'],
                               row['minute'], row['load_act'])
    conn.commit()



def get_sp_to_gmt(ld):

    ld.sp_date = pd.to_datetime(ld.sp_date, format='%Y%m%d')
    ld['sp_date_new'] = ld['sp_date']
    
    #timechange date correction
    for i in range(2013,2021):
        # march timechange
        last_sun = max(week[-1] for week in calendar.monthcalendar(i, 3))
        ld.loc[(ld['sp_date_new']==datetime(i,3,last_sun)) & (ld['sp']>1)  & (ld['sp']<47),'sp'] = ld[(ld['sp_date_new']==datetime(i,3,last_sun)) & (ld['sp']>1) & (ld['sp']<47)]['sp']+2
        #oct timechange
        last_sun = max(week[-1] for week in calendar.monthcalendar(i, 10))
        ld.loc[(ld['sp_date_new']==datetime(i,10,last_sun)) & (ld['sp']>3),'sp'] = ld[(ld['sp_date_new']==datetime(i,10,last_sun)) & (ld['sp']>3)]['sp']-2
        print(ld[(ld['sp_date_new']==datetime(i,10,last_sun)) & (ld['sp']>3)]['sp']-2)

        
    ld['hour'] = ld.sp*30//60
    ld['minute'] = ld.sp*30%60
    
    ld.loc[ld.hour==24, 'sp_date_new'] = ld.loc[ld.hour==24, 'sp_date_new'] + timedelta(days = 1)
    ld.loc[ld.hour==24, 'hour'] = 0
    
    ld['sp_datetime'] = ld.sp_date_new
    ld['datetime'] = ld.sp_date_new
    
    for ind, row in ld.iterrows():

        ld.loc[ind, 'sp_datetime'] = row.sp_date_new+ timedelta(hours = row.hour)+ timedelta(minutes = row.minute)

        # convert to utc
        if (load.loc[ind, 'sp_datetime'].month==10) & (load.loc[ind, 'sp_datetime'].day==max(week[-1] for week in calendar.monthcalendar(load.loc[ind, 'sp_datetime'].year, 10))):
            
            if (ld.loc[ind, 'sp']==48) | (ld.loc[ind, 'sp']<=3):
                
                ld.loc[ind, 'datetime'] = ld.loc[ind, 'sp_datetime']-timedelta(hours = 1)

                if load.loc[ind,'sp_datetime']==load.loc[ind-2,'sp_datetime']:
                    ld.loc[ind, 'datetime'] = ld.loc[ind, 'sp_datetime']
                print(ld.loc[ind, 'datetime'])
            else:
                ld.loc[ind, 'datetime'] = ld.loc[ind, 'sp_datetime']
                if (load.loc[ind,'sp_datetime']-load.loc[(ind-1),'sp_datetime'])<timedelta(minutes=30):
                    ld.loc[ind, 'datetime'] = ld.loc[ind, 'sp_datetime']
        else:
            naive = datetime.fromisoformat(str(ld.loc[ind, 'sp_datetime']))
            local_dt = local.localize(naive, is_dst= None)
            ld.loc[ind, 'datetime'] = local_dt.astimezone(pytz.utc)

        
    ld['day'] = pd.to_datetime(ld['datetime'],utc=True).dt.day
    ld['month'] = pd.to_datetime(ld['datetime'],utc=True).dt.month
    ld['year'] = pd.to_datetime(ld['datetime'],utc=True).dt.year
    ld['hour'] = pd.to_datetime(ld['datetime'],utc=True).dt.hour
    ld['minute'] = pd.to_datetime(ld['datetime'],utc=True).dt.minute
    return ld

for d in range(29, 59, 30): # updated up until 2020-08-23
    end_date =  (datetime.now()-timedelta(days = d)).strftime("%Y-%m-%d") #'2020-06-01'
    start_date = (datetime.now()-timedelta(days = (d+30))).strftime("%Y-%m-%d")
    thedata = get_raw_data(StartDate = start_date, StopDate=end_date)
    load = pd.DataFrame(process_raw_data (thedata), columns=['sp_date', 'sp', 'load_act'])
    load['sp'] = load['sp'].astype(str).astype(int)
    ld=get_sp_to_gmt(load) 
    print(ld)
    update_gmt_data (ld, conn)

