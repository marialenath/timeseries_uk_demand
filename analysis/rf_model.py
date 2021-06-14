import numpy as np
import pandas as pd
import pyodbc
from pathlib import Path 
import sys
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from datetime import datetime, date
import matplotlib.pyplot as plt
import joblib

#
par_dir = str(Path.cwd())

sys.path.append(par_dir + '\/features')
from engineering import get_features

sys.path.append(par_dir + '\data')
from pull_data_from_db import get_data_from_db

sys.path.append(par_dir + '\evaluation')
from evaluate import get_stats

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')

# Pull data from database
data = get_data_from_db(conn) 

# Feature engineer: seasonals, trend, extra temp variables
data = get_features(data, daysback=3000)


# Take ln(np.log) of load_act
data['tr_load_act'] = np.log(data['load_act'])
data.reset_index(drop=True, inplace=True)
data['load_act_prev_h'] = data['load_act'].rolling(2).mean().shift(1)
data['load_act_prev_h6'] = data['load_act'].rolling(6).mean().shift(1)
data['temp_lhr_prev_h'] = data['tempk_heathrow'].rolling(2).mean().shift(1)
data['temp_lhr_prev_h6'] = data['tempk_heathrow'].rolling(6).mean().shift(1)
data['temp_lhr_max'] = data['tempk_heathrow'].rolling(48).max().shift(1)
data['temp_lhr_min'] = data['tempk_heathrow'].rolling(48).min().shift(1)


# split to test and train
test = data[(data['dt']>=datetime.date(datetime(2020,1,1))) & (data['dt']<=datetime.date(datetime(2020,2,15)))]

train = data[(data['dt']>=datetime.date(datetime(2017,1,1))) & (data['dt']<datetime.date(datetime(2020,1,1)))]


def rf_model(test, train):
    '''
    RF model: seasonal variables, bank/school holiday variables, temperature,
    wind speed, solar radiation, installed capacity
    '''

    rf_model_X = ['month', 'time', 'weekday', 
                    'tempk_heathrow', 'wind_mps_heathrow', 'rad_jm2_heathrow',
                    'embedded_wind_cpacity', 'embedded_solar_capacity',
                    'tempk_bramham', 'wind_mps_bramham', 'rad_jm2_bramham',
                    'tempk_bishopton', 'wind_mps_bishopton', 'rad_jm2_bishopton',
                    'tempk_coleshill', 'wind_mps_coleshill', 'rad_jm2_coleshill',
                    'tempk_devon', 'wind_mps_devon', 'rad_jm2_devon',
                    'all_hol', 'school_hol']
    rf_model = RandomForestRegressor(max_features='auto', max_depth=10, min_samples_split=45, n_estimators=200, random_state=0)
    rf_model.fit(train[rf_model_X], train['tr_load_act'])
    test.loc[:, 'rf_model'] =  np.exp(rf_model.predict(test[rf_model_X]))
    
    importances = rf_model.feature_importances_
    indices = np.argsort(importances)


    plt.title('Feature Importances')
    plt.barh(range(len(indices)), importances[indices], color='b', align='center')
    plt.yticks(range(len(indices)), [rf_model_X[i] for i in indices])
    plt.xlabel('Relative Importance')
    plt.show()
    
    return test, rf_model

# random forest
test, rf_model = rf_model(test, train)
stats = get_stats(test, model_name = 'rf_model')
stats.to_csv('rf_model_stats.csv', index=False)
test[['dt', 'time', 'load_act', 'rf_model']].to_csv('rf_model.csv', index=False)

# save model
filename = 'rf_model.sav'
joblib.dump(rf_model, filename)

