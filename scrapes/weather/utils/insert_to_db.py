# -*- coding: utf-8 -*-


import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=;'
                      'Database=dissertation;'
                      'Trusted_Connection=yes;')

def update_lndsynop2(conn, cleaned):
    cursor = conn.cursor()
    
    for index,row in cleaned.iterrows():
        cursor.execute("""INSERT INTO dissertation.dbo.lndsynop(
                            [station], 
                            [year],[month],[day],[hour],
                            [minute],[wdir],[wind_speed],[tempk],
                            [humid],[rain],[rad_jm2],[cloud8])
                            values (?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                           row['Station'], 
                           row['Year'], row['Month'], row['Day'], 
                           row['Hour'], row['Minute'], 
                           row['Wind direction'], row['Wind speed'], row['Air temperature'], 
                           row['Relative humidity'], row['Section 1 rainfall amount'], 
                           row['global radiation in last hour'], row['Cloud amount (8 groups)'])
    conn.commit()



def update_lndsynop(conn, cleaned):
    cursor = conn.cursor()
    
    for index,row in cleaned.iterrows():

        try:
            cursor.execute("""INSERT INTO dissertation.dbo.lndsynop(
                                [station], 
                                [year],[month],[day],[hour],
                                [minute],[wdir],[wind_speed],[tempk],
                                [humid],[rain],[rad_jm2],[cloud8])
                                values (?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                               row['Station'], 
                               row['Year'], row['Month'], row['Day'], 
                               row['Hour'], row['Minute'], 
                               row['Wind direction'], row['Wind speed'], row['Air temperature'], 
                               row['Relative humidity'], row['Section 1 rainfall amount'], 
                               row['global radiation in last hour'], row['Cloud amount (8 groups)'])
        except:
            cursor.execute(f"""DELETE FROM dissertation.dbo.lndsynop 
               where station='{row['Station']}' and year = {row['Year']}
               and month = {row['Month']} and day = {row['Day']}
               and hour = {row['Hour']} and minute = {row['Minute']}""")
            
            cursor.execute("""INSERT INTO dissertation.dbo.lndsynop(
                            [station], 
                            [year],[month],[day],[hour],
                            [minute],[wdir],[wind_speed],[tempk],
                            [humid],[rain],[rad_jm2],[cloud8])
                            values (?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                           row['Station'], 
                           row['Year'], row['Month'], row['Day'], 
                           row['Hour'], row['Minute'], 
                           row['Wind direction'], row['Wind speed'], row['Air temperature'], 
                           row['Relative humidity'], row['Section 1 rainfall amount'], 
                           row['global radiation in last hour'], row['Cloud amount (8 groups)'])
    conn.commit()
    
    
def update_midas(conn, cleaned):
    cursor = conn.cursor()
    for index,row in cleaned.iterrows():
        try:
            cursor.execute("""INSERT INTO dissertation.dbo.midas_open(
                                    [station], 
                                    [year],[month],[day],
                                    [hour], [minute],
                                    [wdir],[windk],[tempc],
                                    [cloud8],[humid],[rad_kjm2])
                                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                                   row['station'], 
                                   row['year'], row['month'], row['day'], 
                                   row['hour'], row['minute'], 
                                   row['wdir'], row['windk'], row['tempc'], 
                                   row['cloud8'], row['humid'], row['rad_kjm2'])
        except:
            cursor.execute(f"""DELETE FROM dissertation.dbo.midas_open
               where station='{row['Station']}' and year = {row['Year']}
               and month = {row['Month']} and day = {row['Day']}
               and hour = {row['Hour']} and minute = {row['Minute']}""")
            
            cursor.execute("""INSERT INTO dissertation.dbo.midas_open(
                                    [station], 
                                    [year],[month],[day],
                                    [hour], [minute],
                                    [wdir],[windk],[tempc],
                                    [cloud8],[humid],[rad_kjm2])
                                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                                   row['station'], 
                                   row['year'], row['month'], row['day'], 
                                   row['hour'], row['minute'], 
                                   row['wdir'], row['windk'], row['tempc'], 
                                   row['cloud8'], row['humid'], row['rad_kjm2'])
    conn.commit()
	
    # replace -9999999 with null
    cursor.execute("""UPDATE dissertation.dbo.midas_open set wdir= nullif(wdir,-9999999)
		where wdir=-9999999""")
    
    cursor.execute("""UPDATE dissertation.dbo.midas_open set windk= nullif(windk,-9999999)
		where windk=-9999999""")
    
    cursor.execute("""UPDATE dissertation.dbo.midas_open set tempc= nullif(tempc,-9999999)
		where tempc=-9999999""")
    cursor.execute("""UPDATE dissertation.dbo.midas_open set cloud8= nullif(cloud8,-9999999)
		where cloud8=-9999999""")
    cursor.execute("""UPDATE dissertation.dbo.midas_open set humid= nullif(humid,-9999999)
		where humid=-9999999""")
    cursor.execute("""UPDATE dissertation.dbo.midas_open set rad_kjm2= nullif(rad_kjm2,-9999999)
		where rad_kjm2=-9999999""")
    conn.commit()
    
