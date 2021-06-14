import numpy as np
import pandas as pd
import pyodbc
from pathlib import Path 
import sys
from sklearn.model_selection import TimeSeriesSplit
from datetime import datetime, date
import joblib
from sklearn.ensemble import RandomForestRegressor
from numpy import split
from numpy import array
from sklearn.metrics import mean_squared_error
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Flatten
from keras.layers import LSTM
from keras.layers import RepeatVector
from keras.layers import TimeDistributed
from keras.layers import Dropout
from sklearn.preprocessing import MinMaxScaler
from keras import regularizers
from sklearn import decomposition
from keras.callbacks import EarlyStopping
import tensorflow as tf
seed_value= 101
# 1. Set `PYTHONHASHSEED` environment variable at a fixed value
import os
os.environ['PYTHONHASHSEED']=str(seed_value)

# 2. Set `python` built-in pseudo-random generator at a fixed value
import random
random.seed(seed_value)

# 3. Set `numpy` pseudo-random generator at a fixed value
np.random.seed(seed_value)

# 4. Set the `tensorflow` pseudo-random generator at a fixed value
tf.compat.v1.set_random_seed(seed_value)

# 5. Configure a new global `tensorflow` session
from keras import backend as K
session_conf = tf.ConfigProto(intra_op_parallelism_threads=1, inter_op_parallelism_threads=1)
sess = tf.Session(graph=tf.get_default_graph(), config=session_conf)
K.set_session(sess)
np.random.seed(101)
tf.compat.v1.random.set_random_seed(101)
#
par_dir = str(Path.cwd())

sys.path.append(par_dir + '\/features')
from engineering import get_features

sys.path.append(par_dir + '\data')
from pull_data_from_db import get_data_from_db

sys.path.append(par_dir + '\evaluation')
from evaluate import get_stats


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server='
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')

# Pull data from database
data = get_data_from_db(conn) 

# Feature engineer: seasonals, trend, extra temp variables
data = get_features(data, daysback=1455)


# Take ln(np.log) of load_act
data['tr_load_act'] = np.log(data['load_act'])
data.reset_index(drop=True, inplace=True)

# split to test and train
test = data[(data['dt']>=datetime.date(datetime(2020,1,1))) & (data['dt']<=datetime.date(datetime(2020,2,15)))]

train = data[(data['dt']>=datetime.date(datetime(2018,1,1))) & (data['dt']<datetime.date(datetime(2020,1,1)))]


def preprocess(train, test, nn_x, nn_y, features_to_be_scaled, timesteps = 48):
    scaler = MinMaxScaler()
    yscaler = MinMaxScaler()
    
    new_train = train
    new_train[features_to_be_scaled] = scaler.fit_transform(train[features_to_be_scaled])
    new_train[nn_y] = yscaler.fit_transform(train[nn_y])
    new_train = new_train[nn_x]
    
    new_test = test
    new_test[features_to_be_scaled] = scaler.transform(test[features_to_be_scaled])
    new_test[nn_y] = yscaler.transform(test[nn_y])
    new_test = new_test[nn_x]
    
    # pca
    pca = decomposition.PCA().fit(new_train)
    import matplotlib.pyplot as p
    p.plot(np.cumsum(pca.explained_variance_ratio_))
    p.xlabel('number of components')
    p.ylabel('cumulative explained variance');


    pca = decomposition.PCA(n_components=20)
    nntrainx = pca.fit_transform(new_train)
    nntestx = pca.transform(new_test)
    
    # refromat data to have # days*years samples, 48 timesteps and len(nn_x) # of features
    #nntrainx = new_train.to_numpy()
    #nntestx = new_test.to_numpy()
    
    new_nntrainx = array(split(nntrainx, len(nntrainx)/timesteps))
    new_nntrainy = np.array(train[nn_y]).reshape(new_nntrainx.shape[0], timesteps, 1)
    
    new_nntestx = array(split(nntestx, len(nntestx)/timesteps))
    new_nntesty = np.array(test[nn_y]).reshape(new_nntestx.shape[0], timesteps, 1)
    return new_nntrainx, new_nntrainy, new_nntestx, new_nntesty, yscaler


train = train.sort_values(by=['year', 'month', 'day', 'hour', 'minute'])
test = test.sort_values(by=['year', 'month', 'day', 'hour', 'minute'])
nn_x = ['s2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10', 's11', 's12',
        'time', 'weekday',
        'embedded_wind_cpacity', 'embedded_solar_capacity',
    	'tempk_heathrow', 'wind_mps_heathrow', 'rad_jm2_heathrow',
    	'tempk_bramham', 'wind_mps_bramham', 'rad_jm2_bramham',
    	'tempk_bishopton', 'wind_mps_bishopton', 'rad_jm2_bishopton',
    	'tempk_coleshill', 'wind_mps_coleshill', 'rad_jm2_coleshill',
    	'tempk_devon', 'wind_mps_devon', 'rad_jm2_devon',
    	'all_hol','school_hol', 'temp_lhr_prev_h']

nn_y = ['tr_load_act']

features_to_be_scaled = ['time', 'weekday',
        'embedded_wind_cpacity', 'embedded_solar_capacity',
    	'tempk_heathrow', 'wind_mps_heathrow', 'rad_jm2_heathrow',
    	'tempk_bramham', 'wind_mps_bramham', 'rad_jm2_bramham',
    	'tempk_bishopton', 'wind_mps_bishopton', 'rad_jm2_bishopton',
    	'tempk_coleshill', 'wind_mps_coleshill', 'rad_jm2_coleshill',
    	'tempk_devon', 'wind_mps_devon', 'rad_jm2_devon',
    	'temp_lhr_prev_h']

new_nntrainx, new_nntrainy, new_nntestx, new_nntesty, yscaler = preprocess(train, test, nn_x, nn_y, features_to_be_scaled)

timesteps=48

model = Sequential()
model.add(LSTM(20, activation='relu', input_shape=(timesteps,new_nntrainx.shape[2]), return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(5, activation='relu', return_sequences=True))
model.add(Dense(1))

from keras.optimizers import Adam
opt =Adam(lr=0.001, beta_1=0.8)
model.compile(optimizer=opt, loss='mse')
model.fit(new_nntrainx, new_nntrainy, epochs=200, validation_split=0.2, verbose=1, batch_size=16,
           callbacks = [EarlyStopping(monitor='val_loss', patience=20)])
res = model.predict(new_nntestx, verbose=0)
model.summary()

test.loc[:, 'van_pca_lstm'] = np.exp(yscaler.inverse_transform(res.reshape(res.shape[0]*res.shape[1], 1)))
stats = get_stats(test, model_name = 'van_pca_lstm')
stats.to_csv('van_pca_lstm_stats.csv', index=False)
test[['dt', 'time', 'load_act', 'van_pca_lstm']].to_csv('van_pca_lstm.csv', index=False)

# save model
filename = 'van_pca_lstm.sav'
joblib.dump(model, filename)


