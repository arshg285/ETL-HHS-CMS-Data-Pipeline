import numpy as np
import pandas as pd
import datetime

def data_cleaning_hhs(file):
    missing_values = [-999999, "NaN", "Not Available", "nan"]
    hhs_data = pd.read_csv(file, na_values=missing_values)

    # Data type converting
    # Change the date
    for i in range(len(hhs_data['collection_week'])):
        temp = datetime.datetime.strptime(hhs_data['collection_week'][i], '%Y-%m-%d')
        hhs_data['collection_week'][i] = temp.date()

    # Change the zip
    hhs_data['zip'] = hhs_data['zip'].astype(str)
    return hhs_data

def data_cleaning_hginfo(date, hginfo):
    
    # Missing value
    missing_values = [-999999, "NaN", "Not Available", "nan"]
    hginfo = pd.read_csv(hginfo, na_values=missing_values)

    #date_input
    try:
       hginfo['collection_week'] = datetime.datetime.strptime(date, '%Y-%m-%d')
    except:
        raise ValueError("Incorrect data format, should be YYYY-mm-dd")
    return hginfo
