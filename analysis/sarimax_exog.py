import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
np.random.seed(101)
tf.compat.v1.random.set_random_seed(101)
from datetime import datetime, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
import joblib
import sys
from pathlib import Path
from statsmodels.graphics.tsaplots import plot_acf, 
par_dir = str(Path.cwd())
sys.path.append(par_dir + '\evaluation')
from evaluate import get_stats

data = pd.read_csv('load_Dec19_Mar20.csv')
data['dt'] = pd.to_datetime(data.dt)
data['dow'] = data['dt'].dt.dayofweek
data['dt'] = data['dt'].dt.date
data['tempk_heathrow_sq'] = data['tempk_heathrow']**2
data['tempk_bishopton_sq'] = data['tempk_bishopton']**2


vars = ['tempk_heathrow_sq', 'tempk_bishopton_sq', 'dow','all_hol', 'school_hol']

# seleced parameters
order = (2, 1, 0)
seasonal_order = (1, 0, 1, 48)

# Loop through 30 dy windows and forecast the next 48 hours
scaler = MinMaxScaler()
fcst = list()

for i in range(46):
  start = datetime(2019, 12, 1)+ timedelta(days=i)
  end =  start+ timedelta(days=30)
  print(f'start: {start} ------- end:{end}')
  
  
  train = data[(data.dt>=datetime.date(start)) & (data.dt<=datetime.date(end))]
  test = data[(data.dt==datetime.date(end+timedelta(days=1)))]
  
  train['load_act'] = scaler.fit_transform(train[['load_act']])
  test['load_act'] = scaler.transform(test[['load_act']])

  model = SARIMAX(endog=train['load_act'], exog = train[vars], order=order, 
                  seasonal_order=seasonal_order, initial_state_intercept=1, 
                  initialization='approximate_diffuse', trend='c')
  results = model.fit()
  print(results.summary())

  sublist = scaler.inverse_transform(results.forecast(steps = 48,exog = test[vars]).values.reshape(-1, 1)).tolist()
  fcst = fcst + [i[0] for i in sublist]

  
test = data[(data.dt>=datetime.date(datetime(2020,1,1))) & (data.dt<=datetime.date(datetime(2020,2, 15)))][['dt', 'year', 'month', 'day', 'hour', 'minute', 'load_act']]
test['fcst'] = fcst

test.to_csv('sarima_exog.csv', index=False)
stats = get_stats(test, model_name = 'fcst')
stats.to_csv('sarima_exog_stats.csv', index=False)
test[['dt', 'hour', 'minute', 'load_act', 'fcst']].to_csv('sarima_exog_test.csv', index=False)

filename = 'sarima_exog.sav'
joblib.dump(model, filename)
