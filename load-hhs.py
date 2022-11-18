import psycopg
import credentials
import numpy as np
import pandas as pd
import sys
import datetime
from data_cleaning import data_cleaning_hhs

# Read and clean the data
hhs = data_cleaning_hhs('/Users/zoe/Desktop/Data engineering/Project/data_engineering_project/hhs_weekly_data_files/' + sys.argv[1])

# Establishing SQL connection
conn = psycopg.connect(
    host = "sculptor.stat.cmu.edu",
    dbname = "arshg",
    user = %(username)s,
    password = %(password)s,
    params = {'username' : username,
              'password' : password}
)

# Creating a cursor object
cur = conn.cursor()

col_name = list(hhs.columns.values)
index_adrs = [col_name.index("hospital_name"),col_name.index("hospital_pk"),col_name.index("city"),col_name.index("state"),col_name.index("zip"),col_name.index("fips_code")]
index_adrs

error_rows_hhs = pd.DataFrame()
num_rows_inserted = 0
num_rows_error_hhs = 0

## Using try-except blocks to perform transactions and insert rows into the database
with conn.transaction():
    for i in range(hhs.shape[0]):
        
        # Make a new savepoint: Adding values to address table
        try:
            cur.execute("INSERT INTO address (hospital_name, hospital_pk, city,state, zip,fips_code) " 
                        "VALUES ( %s, %s,%s,%s,%s,%s)", (hhs.iloc[i,4], hhs.iloc[i,0], hhs.iloc[i,6], hhs.iloc[i,2], hhs.iloc[i,7], hhs.iloc[i, 9]))

        except Exception as e:
            error_rows_hhs.append(e)
            num_rows_error_hhs += 1
            print("Insert failed")

        else:
            num_rows_inserted += 1

        # Make a new savepoint: Adding values to capacity_info table
        try:
            with conn.transaction():
                cur.execute("INSERT INTO capacity_info(hospital_pk, adult_hospital_beds, pediatric_inpatient_beds,adult_hospital_inpatient_bed_occupied, pediatric_inpatient_bed_occupied,total_icu_beds, icu_beds_used )" 
                            "VALUES ( %s, %s,%s,%s,%s,%s,%s)", (hhs.iloc[i,0], float(hhs.iloc[i,12]), float(hhs.iloc[i,111]), float(hhs.iloc[i,55]), float(hhs.iloc[i,108]), float(hhs.iloc[i,22]), float(hhs.iloc[i,24])))

        except Exception as e:
            error_rows_hhs.append(e)
            num_rows_error_hhs += 1
            print("Insert failed")

        else:
            num_rows_inserted += 1
            
        # Make a new savepoint: Adding values to covid_info table
        try:
            with conn.transaction():
                cur.execute("INSERT INTO COVID_info(hospital_name, hospital_pk, inpatient_beds_used_covid_7_day_avg,staffed_adult_icu_patients_confirmed_covid_7_day_avg, total_icu_beds_7_day_avg,icu_beds_used_7_day_avg)" 
               "VALUES ( %s, %s,%s,%s,%s,%s)", (hhs.iloc[i,4], hhs.iloc[i,0], float(hhs.iloc[i,16]), float(hhs.iloc[i,27]), float(hhs.iloc[i,22]), float(hhs.iloc[i, 24])))
                
        except Exception as e:
            error_rows_hhs.append(e)
            num_rows_error_hhs += 1
            print("Insert failed")
            
        else:
            num_rows_successfully_inserted_hhs += 1
            
        # Make a new savepoint: Adding values to ratings table
        try:
            with conn.transaction():
                cur.execute("INSERT INTO ratings(hospital_pk, hospital_name)" 
                            "VALUES ( %s, %s)", (hhs.iloc[i,0], hhs.iloc[i,4]))
                
        except Exception as e:
            error_rows_hhs.append(e)
            num_rows_error_hhs += 1
            print("Insert failed")
            
        else:
            num_rows_successfully_inserted_hhs += 1

# Committing the transaction
conn.commit()

# Saving the data frame containing the error files to a separate CSV file
error_rows_hhs.to_csv("Error rows in HHS data set.csv", index = False)

# Printing the summary output
print("Number of rows successfully inserted:", num_rows_successfully_inserted_hhs)
print("Number of rows unable to be inserted due to errors:", num_rows_error_hhs)

# Closing the SQL connection
conn.close()
