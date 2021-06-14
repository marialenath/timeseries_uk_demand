import numpy as np
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
import matplotlib.pyplot as plt

def xtra_tree_feat_selection(train):
    '''
    Use ExtraTreesRegressor to get feature importances
    '''

    rf_model_X = ['month', 'time', 'weekday', 
                    'tempk_heathrow', 'wind_mps_heathrow', 'rad_jm2_heathrow',
                    'embedded_wind_cpacity', 'embedded_solar_capacity',
                    'tempk_bramham', 'wind_mps_bramham', 'rad_jm2_bramham',
                    'tempk_bishopton', 'wind_mps_bishopton', 'rad_jm2_bishopton',
                    'tempk_coleshill', 'wind_mps_coleshill', 'rad_jm2_coleshill',
                    'tempk_devon', 'wind_mps_devon', 'rad_jm2_devon',
                    'all_hol', 'school_hol']
    rf_model = ExtraTreesRegressor(random_state=0)
    rf_model.fit(train[rf_model_X], train['tr_load_act'])
    
    importances = rf_model.feature_importances_
    indices = np.argsort(importances)

    plt.title('Feature Importances')
    plt.barh(range(len(indices)), importances[indices], color='b', align='center')
    plt.yticks(range(len(indices)), [rf_model_X[i] for i in indices])
    plt.xlabel('Relative Importance')
    plt.show()
    
    return None
	
def correlation(train):
	subs = train[['month', 'time', 'weekday', 
                    'tempk_heathrow', 'wind_mps_heathrow', 'rad_jm2_heathrow',
                    'embedded_wind_cpacity', 'embedded_solar_capacity',
                    'tempk_bramham', 'wind_mps_bramham', 'rad_jm2_bramham',
                    'tempk_bishopton', 'wind_mps_bishopton', 'rad_jm2_bishopton',
                    'tempk_coleshill', 'wind_mps_coleshill', 'rad_jm2_coleshill',
                    'tempk_devon', 'wind_mps_devon', 'rad_jm2_devon','tr_load_act',
                    'all_hol', 'school_hol']]
	return subs.corr()

