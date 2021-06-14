import pandas as pd
import numpy as np

def get_stats(test, model_name, lst):
    lst = []
    for i in test.dt.unique():
        temp = test[test['dt']==i]
        temp['ape'] = ((temp[model_name] - temp['load_act']).abs())/temp['load_act']
        temp['se'] = (temp[model_name] - temp['load_act'])**2
        lst.append([model_name, i, temp['ape'].mean()*100,  (temp['se'].mean())**(1/2)])
    return pd.DataFrame(lst, columns=['model', 'date', 'mape', 'rmse']), lst


