from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from sklearn.metrics import mean_squared_error
import numpy as np
from sklearn.ensemble import RandomForestRegressor


def tune_rf(train):
    rf_model_X = ['month', 'trend', 'time', 'weekday', 
                        'tempk_heathrow', 'wind_mps_heathrow', 'rad_jm2_heathrow',
                        'embedded_wind_cpacity', 'embedded_solar_capacity',
                        'tempk_bramham', 'wind_mps_bramham', 'rad_jm2_bramham',
                        'tempk_bishopton', 'wind_mps_bishopton', 'rad_jm2_bishopton',
                        'tempk_coleshill', 'wind_mps_coleshill', 'rad_jm2_coleshill',
                        'tempk_devon', 'wind_mps_devon', 'rad_jm2_devon',
                        'all_hol', 'covid', 'school_hol'] 
    
    model = RandomForestRegressor(random_state=101)
    #param_search = {'n_estimators': [100, 200], 'max_depth' : [3, 5, 7, 10], 'max_features': ['auto'], 'min_samples_split': [20, 25, 30, 40, 45]}
    param_search = {'n_estimators': [200, 500], 'max_depth' : [5, 7, 8, 10, 12], 'max_features': ['auto', 'sqrt', 'log2'], 'min_samples_split': [30, 40, 45, 50, 55]}
    
    tscv = TimeSeriesSplit(n_splits=4)
    gsearch = GridSearchCV(estimator=model, cv=tscv,
                            param_grid=param_search)
    gsearch.fit(train[rf_model_X], train['tr_load_act'],verbose=1)
    return gsearch.best_params_