import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
np.random.seed(101)
tf.compat.v1.random.set_random_seed(101)
from datetime import datetime, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pyodbc
import sys
from pathlib import Path
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

par_dir = str(Path.cwd())

sys.path.append(par_dir + '\data')
from pull_data_from_db import get_load_data

sys.path.append(par_dir + '\evaluation')
from evaluate import get_stats


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')


# get data
data = get_load_data(conn)
data['dt'] = pd.to_datetime(data[['year', 'month', 'day', 'hour', 'minute']]).dt.date


# get first diff and ACF and PACF plots
data['diff'] = data['load_act'].diff()
train = data[(data.dt>=datetime.date(datetime(2019,12,1))) & (data.dt<=datetime.date(datetime(2019,12,31)))][['diff']]
plot_pacf(train['diff'],lags=48);
plot_acf(train['diff'], lags=48);


# seleced parameters
order = (2, 1, 0)
seasonal_order = (1, 0, 1, 48)

# Loop through 30 dy windows and forecast the next 48 hours
scaler = MinMaxScaler()
fcst = list()
dates_lst = list()

for i in range(45):
  start = datetime(2019, 12, 1)+ timedelta(days=i)
  end =  start+ timedelta(days=30)
  print(f'start: {start} ------- end:{end}')
  
  train = data[(data.dt>=datetime.date(start)) & (data.dt<=datetime.date(end))]
  test = data[data.dt==datetime.date(end+timedelta(days=1))]
  train = train.sort_values(by=['year', 'month', 'day', 'hour', 'minute'])
  test = test.sort_values(by=['year', 'month', 'day', 'hour', 'minute'])

 
  train['load_act'] = scaler.fit_transform(train[['load_act']])
  test['load_act'] = scaler.transform(test[['load_act']])

  model = SARIMAX(endog=train['load_act'], order=order, seasonal_order=seasonal_order, initial_state_intercept=1,  trend='c')
  results = model.fit()


  sublist = scaler.inverse_transform(results.forecast(steps = 48).values.reshape(-1, 1)).tolist()
  fcst = fcst + [i[0] for i in sublist]
  dates_lst.append(end+timedelta(days=1))
  
test = data[(data.dt>=datetime.date(datetime(2020,1,1))) & (data.dt<=datetime.date(datetime(2020,2,14)))][['dt','year', 'month', 'day', 'hour', 'minute','load_act']]
test['sarima'] = fcst
test.plot.line()
test.to_csv('sarima_test.csv', index=False)

stats = get_stats(test, model_name = 'sarima')
stats.to_csv('sarima_Stats.csv', index=False)
