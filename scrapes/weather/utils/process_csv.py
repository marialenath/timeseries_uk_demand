# -*- coding: utf-8 -*-


import pandas as pd
import csv
import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')

def get_stations(conn):
    mystations = pd.read_sql("select * from dissertation.dbo.lndsynop_stations", conn)
    mystations.columns = ['Station', 'Latitude', 'Longitude']
    return mystations

def get_start_line(wx_path):
    with open(wx_path, 'r')  as fin: 
        reader = csv.reader(fin, delimiter=',')
    
        for line_number, line in enumerate(reader):
            if line[0] == 'data':
                start_line = line_number
    return start_line+1

def get_weather(mystations, wx_path , conn):
    colname_dict = {'Year':'1',
    'Month':'2',
    'Day':'3',
    'Hour':'4',
    'Minute':'5',
    'Time of receipt year':'6',
    'Time of receipt month':'7',
    'Time of receipt day':'8',
    'Time of receipt hour':'9',
    'Time of receipt minute':'10',
    'WMO block number':'11',
    'WMO station number':'12',
    'WMO region':'13',
    'Latitude':'14',
    'Longitude':'15',
    'Station height':'16',
    'Pressure sensor height':'17',
    'Collecting centre':'18',
    'Bulletin ID':'19',
    'Station type':'20',
    'Amendment number':'21',
    'Correction number':'22',
    'Original wind speed units':'23',
    'Wind direction':'24',
    'Wind direction quality control flag':'25',
    'Wind speed':'26',
    'Wind speed quality control flag':'27',
    'Air temperature':'28',
    'Air temperature quality control flag':'29',
    'Dew point temperature':'30',
    'Dew point temperature quality control flag':'31',
    'Relative humidity':'32',
    'Horizontal visibility':'33',
    'Horizontal visibility quality control flag':'34',
    'Vertical visibility':'35',
    'Present weather':'36',
    'Present weather quality control flag':'37',
    'Past weather period':'38',
    'Most significant past weather report':'39',
    'Most significant past weather report quality control flag':'40',
    'Least significant pas weather report':'41',
    'Least significant past weather report quality control flag':'42',
    'Amount of all cloud present':'43',
    'Amount of all cloud present quality control flag':'44',
    'Period when max temperature reported':'45',
    'Maximum air temperature':'46',
    'Period when min temp reported':'47',
    'Minumum air temperature':'48',
    'Minumum grass temperature':'49',
    'State of ground with/without snow':'50',
    'Total snow depth':'51',
    'Section 1 rainfall period':'52',
    'Section 1 rainfall amount':'53',
    'Section 1 rainfall amount quality control flag':'54',
    'Section 3 rainfall period':'55',
    'Section 3 rainfall amount':'56',
    'Section 3 rainfall amount quality control flag':'57',
    'Total precipitation past 24 hours':'58',
    'Start of period of max gust':'59',
    'End of period of max gust':'60',
    'Highest gust recorded':'61',
    'Start of period of high mean wind':'62',
    'End of period of high mean wind':'63',
    'Highest mean wind speed':'64',
    'Pressure at mean sea level':'65',
    'Pressure at station level':'66',
    'Standard pressure level':'67',
    'Geopotential height of agreed level':'68',
    '3 hour pressure tendency':'69',
    '3 Hour pressure tendency quality control flag':'70',
    '3 hour pressure change':'71',
    '3 Hour pressure change quality control flag':'72',
    '24 hour pressure change':'73',
    'Type of evapotranspiration instrument':'74',
    'Evapotranspiration amount':'75',
    'Sunshine duration in last 24 hours':'76',
    'Sunshine duration in last hour':'77',
    'Net radiation in last hour':'78',
    'global radiation in last hour':'79',
    'Diffuse radiation in last hour':'80',
    'Long-wave radiation in last hour':'81',
    'Short-wave radiation in last hour':'82',
    'Net short-wave radiation in last hour':'83',
    'Direct solar radiation in last hour':'84',
    'Net radiation in last 24 hours':'85',
    'global radiation in last 24 hours':'86',
    'Diffuse radiation in last 24 hours':'87',
    'Long-wave radiation in last 24 hours':'88',
    'Short-wave radiation in last 24 hours':'89',
    'Net short-wave radiation in last 24 hours':'90',
    'Direct solar radiation in last 24 hours':'91',
    'Visibility in seaward direction':'92',
    'State of sea':'93',
    'Temperature measuring method':'94',
    'Sea surface temperature':'95',
    'Wave measuring method':'96',
    'Height of wind waves':'97',
    'Period of wind waves':'98',
    'Primary swell wave direction':'99',
    'Primary swell wave period':'100',
    'Primary swell wave height':'101',
    'Secondary swell wave direction':'102',
    'Secondary swell wave period':'103',
    'Secondary swell wave height':'104',
    'Low cloud type (CL)':'105',
    'Low cloud type (CL) quality control flag':'106',
    'Medium cloud type (CM)':'107',
    'Medium cloud type (CM) quality control flag':'108',
    'High cloud type (CH)':'109',
    'High cloud type (CH) quality control flag':'110',
    'Low cloud amount':'111',
    'Low cloud amount quality control flag':'112',
    'Height of base of cloud':'113',
    'High of base of cloud quality control flag':'114',
    'Cloud amount (8 groups)1':'115',
    'Cloud amount quality control flag1':'116',
    'Cloud type (8 groups)1':'117',
    'Cloud type quality control flag1':'118',
    'Height of cloud base (8 groups)1':'119',
    'Height of cloud base quality control flag1':'120',
    'Cloud amount (8 groups)2':'121',
    'Cloud amount quality control flag2':'122',
    'Cloud type (8 groups)2':'123',
    'Cloud type quality control flag2':'124',
    'Height of cloud base (8 groups)2':'125',
    'Height of cloud base quality control flag2':'126',
    'Cloud amount (8 groups)3':'127',
    'Cloud amount quality control flag3':'128',
    'Cloud type (8 groups)3':'129',
    'Cloud type quality control flag3':'130',
    'Height of cloud base (8 groups)3':'131',
    'Height of cloud base quality control flag3':'132',
    'Cloud amount (8 groups)':'133',
    'Cloud amount quality control flag':'134',
    'Cloud type (8 groups)':'135',
    'Cloud type quality control flag':'136',
    'Height of cloud base (8 groups)':'137',
    'Height of cloud base quality control flag':'138',
    'Below station cloud amount':'139',
    'Below station cloud type':'140',
    'Below station cloud base height':'141',
    'Below station cloud base type':'142',
    'Above station cloud amount':'143',
    'Above station cloud type':'144',
    'Above station cloud top height':'145',
    'Above station cloud top type':'146'}
    
    start_line = get_start_line(wx_path)
    df = pd.read_csv(wx_path, skiprows=start_line)[:-1]
    df = df[[colname_dict['Year'],
             colname_dict['Month'],
             colname_dict['Day'],
             colname_dict['Hour'],
             colname_dict['Minute'],
             colname_dict['Latitude'],
             colname_dict['Longitude'],
             colname_dict['Wind direction'],
             colname_dict['Wind speed'],
             colname_dict['Air temperature'],
             colname_dict['Relative humidity'],
             colname_dict['Section 1 rainfall amount'],
             colname_dict['global radiation in last hour'],
             colname_dict['Cloud amount (8 groups)']]]
    new_colnames = []
    for i in df.columns: 
        new_colnames.append(list(colname_dict.keys())[list(colname_dict.values()).index(i)])
        
    df.columns = new_colnames
    
    cleaned =  pd.merge(mystations, df, on=['Latitude', 'Longitude'])
    
    return cleaned
