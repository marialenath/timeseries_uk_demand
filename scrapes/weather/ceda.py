# -*- coding: utf-8 -*-

import ftplib
import os
import pyodbc
from datetime import datetime, timedelta
import pandas as pd
 

ceda_username = ""
ceda_password = ""


# Define the local directory name to put data in
ddir="Documents/EnergyProject/weather/lndsyn"

# login to FTP
f=ftplib.FTP("ftp.ceda.ac.uk", ceda_username, ceda_password)

missing_csv = []

# loop through days back
for d in range(100, 2400): 
    mydate =  (datetime.now()-timedelta(days = d))
    print(mydate)
    
    # change the remote directory
    f.cwd("/badc/ukmo-metdb/data/lndsyn/%.4d/%.2d" % (mydate.year, mydate.month))
    
    # define filename
    file="ukmo-metdb_lndsyn_%.4d%.2d%.2d.csv" % (mydate.year, mydate.month, mydate.day)

    
    try:
        # get the remote file to the local directory
        f.retrbinary("RETR %s" % file, open(file, "wb").write)
        
    except:
        missing_csv.append(file)

            
missing_df = pd.DataFrame(missing_csv, columns=['file'])
missing_df.to_csv('missing_files.csv', index=False)
# Close FTP connection
f.close()