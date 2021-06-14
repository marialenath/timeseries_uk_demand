# -*- coding: utf-8 -*-


import pandas as pd
import datetime as dt
import pyodbc 


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')


start_date = dt.date(2014, 1, 1)
end_date = dt.date(2020, 12, 31)


delta = end_date - start_date 
lst = []
for i in range(delta.days + 1):
    day = start_date + dt.timedelta(days=i)
    lst.append(day)
    
df = pd.DataFrame(lst, columns = ['date'])
df['date'] = pd.to_datetime(df['date'], dayfirst = True)
df.reset_index(drop=True)

def update_school_data (result, conn):
    cursor = conn.cursor()
    for index,row in result.iterrows():
        cursor.execute("""INSERT INTO dissertation.dbo.school_holidays(
                                [day], [month], [year], [school_hol])
                                values (?, ?, ?, ?)""", 
                               row['day'], row['month'], row['year'], row['school_hol'])
    conn.commit()

def update_bank_data (result, conn):
    cursor = conn.cursor()
    for index,row in result.iterrows():
        cursor.execute("""INSERT INTO dissertation.dbo.bank_holidays(
                                [day], [month], [year], 
                                [easter_mon], [easter_mon_pre], 
                                [easter_mon_post], [xmas_day], 
                                [xmas_day_pre], [xmas_day_post], 
                                [boxing_day], [boxing_day_pre], 
                                [boxing_day_post], [good_fri], 
                                [good_fri_pre], [good_fri_post], 
                                [early_may_bank_hol], [early_may_bank_hol_pre], 
                                [early_may_bank_hol_post], [aug_bank_hol], 
                                [aug_bank_hol_pre], [aug_bank_hol_post], 
                                [spring_bank_hol], [spring_bank_hol_pre], 
                                [spring_bank_hol_post], [new_years_day],
                                [new_years_day_pre], [new_years_day_post])
                                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                               row['day'], row['month'], row['year'], 
                               row['easter_mon'], row['easter_mon_pre'],
                               row['easter_mon_post'], row['xmas_day'],
                               row['xmas_day_pre'], row['xmas_day_post'],
                               row['boxing_day'], row['boxing_day_pre'],
                               row['boxing_day_post'], row['good_fri'],
                               row['good_fri_pre'], row['good_fri_post'],
                               row['early_may_bank_hol'], row['early_may_bank_hol_pre'],
                               row['early_may_bank_hol_post'], row['aug_bank_hol'],
                               row['aug_bank_hol_pre'], row['aug_bank_hol_post'],
                               row['spring_bank_hol'], row['spring_bank_hol_pre'],
                               row['spring_bank_hol_post'], row['new_years_day'],
                               row['new_years_day_pre'], row['new_years_day_post'])
    conn.commit()


def add_school_holidays(df, conn):   
    school_hol = pd.read_csv("school_holidays.csv")
    school_hol['Start'] = pd.to_datetime(school_hol['Start'], dayfirst = True)
    school_hol['End'] = pd.to_datetime(school_hol['End'], dayfirst = True)
    school_hol.reset_index(drop=True)
        
    df['school_hol'] = 0
    df.loc[df['date'].dt.day_name().isin(['Saturday', 'Sunday']), 'school_hol'] = 1
    for i, row in school_hol.iterrows():
        print(row['Start'], row['End'])
        df.loc[(df['date'] >= row['Start']) & (df['date'] <= row['End']), 'school_hol'] = 1
    
    df['day'] = df.date.dt.day
    df['month'] = df.date.dt.month
    df['year'] = df.date.dt.year    
        
    update_school_data (df, conn)
    
def add_bank_holidays(df, conn):
    
    bank = pd.read_csv('bank_holidays.csv')
    bank['date'] = pd.to_datetime(bank['date'], dayfirst = True)
    df['day'] = df.date.dt.day
    df['month'] = df.date.dt.month
    df['year'] = df.date.dt.year
    
    for i in set(bank.holiday_name):
        df[i] = 0
        df[i+'_pre'] = 0
        df[i+'_post'] = 0

        for ind, row in bank[bank['holiday_name']==i].iterrows():
            df.loc[df['date']==row['date'], i] = 1
            df.loc[df['date']==row['date']-dt.timedelta(days=1), i+'_pre'] = 1
            df.loc[df['date']==row['date']+dt.timedelta(days=1), i+'_post'] = 1
    update_bank_data (df, conn)
    
    
add_bank_holidays(df, conn)   
    