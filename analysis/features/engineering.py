from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pyodbc
import math

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')



def get_wkdays(date_col, daysback):
    '''Flags weekdays as 0 and weekends as 1'''
    lst = []
    for d in range(1, daysback): 
        mydate =  (datetime.now()-timedelta(days = d))
        wd = 1 if mydate.weekday()>=5 else 0
        
        lst.append([mydate.date(), wd,  mydate.weekday()])
    
    df = pd.DataFrame(lst, columns = [date_col, 'wd', 'weekday'])
    df['year'] = pd.DatetimeIndex(df[date_col]).year
    df['month'] = pd.DatetimeIndex(df[date_col]).month
    df['day'] = pd.DatetimeIndex(df[date_col]).day
    return df

def enhance_weather_vars(data):
    '''
    Created some extra temp variables
    '''
    data.sort_values(by=['year', 'month', 'day', 'hour', 'minute'])
    data['temp_lhr_prev_h'] = data['tempk_heathrow'].rolling(2).mean().shift(1)
    data['temp_lhr_prev_h6'] = data['tempk_heathrow'].rolling(6).mean().shift(1)
	data['temp_lhr_prev_h48'] = data['tempk_heathrow'].rolling(48).mean().shift(1)
    data['temp_lhr_max'] = data['tempk_heathrow'].rolling(48).max().shift(1)
    data['temp_lhr_min'] = data['tempk_heathrow'].rolling(48).min().shift(1)
    data['tempk_heathrow_sq'] = data['tempk_heathrow']**2
    data['tempk_bishopton_sq'] = data['tempk_bishopton']**2
	
    data['rad_jm2_lhr_max'] = data['rad_jm2_heathrow'].rolling(48).max().shift(1)
    data['rad_jm2_lhr_diff'] = data['rad_jm2_heathrow'].diff(1)
	data['rad_jm2_lhr_max3'] = data['rad_jm2_heathrow'].rolling(6).max().shift(1)
	data['rad_jm2_devon_max3'] = data['rad_jm2_devon'].rolling(6).max().shift(1)
	data['rad_jm2_bishopton_max3'] = data['rad_jm2_bishopton'].rolling(6).max().shift(1)

    data['rad_jm2_devon_diff'] = data['rad_jm2_devon'].diff(1)
	
	data['load_prev_h48'] = data['tr_load_act'].shift(48)
	
    return data


def create_blended_weather_station(data):
    '''get weighted average of stations with weights based on their population'''
    station_weights = {'heathrow': 0.622442531,
                       'bishopton': 0.067513996,
                       'bramham': 0.121488168,
                       'coleshill': 0.156636812,
                       'devon': 0.031918493}
    for i in ['tempk']:
        print(i)
        data[i+'_blended'] = station_weights['heathrow'] * data[i+'_heathrow'] 
        + station_weights['bishopton'] * data[i+'_bishopton']
        + station_weights['bramham'] * data[i+'_bramham']
        + station_weights['coleshill'] * data[i+'_coleshill']
        + station_weights['devon'] * data[i+'_devon'] 
    return data
	
def add_week_of_month(df):
    df['week_in_month'] = pd.to_numeric(df.day/7)
    df['week_in_month'] = df['week_in_month'].apply(lambda x: math.ceil(x))
    return df
	

def get_hourly_dummies(data):
    for h in data.time.unique():
        data["H_"+h]=np.where(data['time']==h, 1, 0)
    return data


def get_features(data, daysback):
    '''
    Retrieves seasonal, weekday/weekend dummies, turns weekday/day/month/time to class variables,
    adds enhanced temperature variables and trend    
    '''
    date_col = 'dt'
     
    # add weekdays
    df_weekdays = get_wkdays(date_col, daysback)
    print(df_weekdays.columns)
	
    
    # add seasonal
    df_weekdays_seasonal = get_seasonal_dummies(df_weekdays, date_col, daysback)
    result = pd.merge(df_weekdays_seasonal, data, on= ['year', 'month', 'day'], how = 'inner')
    
    # add trend variable
    result = result.sort_values(by=['year', 'month', 'day', 'hour', 'minute'])
    result['trend'] = 0
    for cnt, d in enumerate(result.dt.unique()):
        result.loc[result.dt==d, 'trend'] = cnt
    
    # create time variable
    result['time'] = pd.to_datetime(result[['year', 'month', 'day', 'hour', 'minute']]).dt.strftime('%H%M')
    
    
    # add extra temp variables
    result = enhance_weather_vars(result)
    
	# add all holiday variable
    result['all_hol'] = result['xmas_day']+result['boxing_day']+ result['good_fri'] + result['easter_mon'] + result['early_may_bank_hol']+ result['spring_bank_hol'] + result['aug_bank_hol'] + result['new_years_day']
    result['all_hol_pre'] = result['xmas_day_pre']+result['boxing_day_pre']+ result['good_fri_pre'] + result['easter_mon_pre'] + result['early_may_bank_hol_pre']+ result['spring_bank_hol_pre'] + result['aug_bank_hol_pre'] + result['new_years_day_pre']
    result['all_hol_post'] = result['xmas_day_post']+result['boxing_day_post']+ result['good_fri_post'] + result['easter_mon_post'] + result['early_may_bank_hol_post']+ result['spring_bank_hol_post'] + result['aug_bank_hol_post'] + result['new_years_day_post']
    
	#time int
    result['time_int'] = result.time.astype(int)
	
	# week num
    result = add_week_of_month(result)
	
    return result

