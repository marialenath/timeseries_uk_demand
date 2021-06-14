import numpy as np
import pandas as pd
import pyodbc
from pathlib import Path 
import sys
from sklearn.model_selection import TimeSeriesSplit
from datetime import datetime, date
import statsmodels.formula.api as smf
import joblib

#
par_dir = str(Path.cwd())

sys.path.append(par_dir + '\/features')
from engineering import get_features

sys.path.append(par_dir + '\data')
from pull_data_from_db import get_data_from_db

sys.path.append(par_dir + '\evaluation')
from evaluate import get_stats

sys.path.append(par_dir + '\\models')
import models2

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



# split to test and train
test = data[(data['dt']>=datetime.date(datetime(2020,1,1))) & (data['dt']<=datetime.date(datetime(2020,2,15)))]

train = data[(data['dt']>=datetime.date(datetime(2017,1,1))) & (data['dt']<datetime.date(datetime(2020,1,1)))]



def vanilla_model(test, train):
    '''
    Tao Hong's vanilla model used as a benchmark to GEF Competition 2012
    1)main effects: Trend, temperature, Month, Weekday, Hour and 
    with cross effects (interactions): Hour*Weekday, T*Month, (T**3)*Month, 
    (T**2)*Month, T*Hour, (T**3)*Hour and (T**2)*Hour.
    '''
    vanilla = '''
    tr_load_act ~ trend + month + time + weekday + weekday*time + 
                tempk_heathrow + tempk_heathrow**2 + tempk_heathrow**3 +
                month*tempk_heathrow + month*tempk_heathrow**2 + month*tempk_heathrow**3 +
                time*tempk_heathrow + time*tempk_heathrow**2 + time*tempk_heathrow**3 +
                tempk_bramham + tempk_bramham**2 + tempk_bramham**3 +
                month*tempk_bramham + month*tempk_bramham**2 + month*tempk_bramham**3 +
                time*tempk_bramham + time*tempk_bramham**2 + time*tempk_bramham**3 +
                tempk_bishopton + tempk_bishopton**2 + tempk_bishopton**3 +
                month*tempk_bishopton + month*tempk_bishopton**2 + month*tempk_bishopton**3 +
                time*tempk_bishopton + time*tempk_bishopton**2 + time*tempk_bishopton**3 +
                tempk_coleshill + tempk_coleshill**2 + tempk_coleshill**3+
                month*tempk_coleshill + month*tempk_coleshill**2 + month*tempk_coleshill**3+
                time*tempk_coleshill + time*tempk_coleshill**2 + time*tempk_coleshill**3
    '''
    vanilla_X = ['trend', 'month', 'time', 'weekday', 'tempk_heathrow',
                 'tempk_bramham', 'tempk_bishopton', 'tempk_coleshill']
    vanilla_model = smf.ols(formula= vanilla, data=train).fit()
    vanilla_model.summary()
    test.loc[:, 'vanilla_model'] =  np.exp(vanilla_model.predict(test[vanilla_X]))
    
    return test, vanilla_model

# Tao Hong's vanilla model
test, vanilla_model = vanilla_model(test, train)
stats = get_stats(test, model_name = 'vanilla_model')
stats.to_csv('vanilla_linear_model_stats.csv', index=False)
test[['dt', 'time', 'load_act', 'vanilla_model']].to_csv('vanilla_linear_model.csv', index=False)

# save model
filename = 'vanilla_linear_model.sav'
joblib.dump(model, filename)
