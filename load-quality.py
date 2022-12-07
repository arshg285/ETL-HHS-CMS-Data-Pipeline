# Importing libraries
import psycopg
import pandas as pd
import requests
import time
import io
from credentials import username, password
import sys
from data_cleaning import data_cleaning_hginfo

# Data loading and cleaning
input_date = sys.argv[1]
cms_file = sys.argv[2]
file = str('/Users/arshmacbook/Desktop/36-614/Project/hospital_quality_files/' + cms_file) # Change this to user directory
cms = data_cleaning_hginfo(input_date, file)
cms['emergency_services_provided'] = cms['Emergency Services'].map({'Yes': True, 'No': False})

# Establishing SQL connection
conn = psycopg.connect(
    host = "sculptor.stat.cmu.edu",
    dbname = "arshg",
    user = username,
    password = password
)

# Creating a cursor object
cur = conn.cursor()

error_rows_cms = pd.DataFrame()
num_rows_successfully_inserted_cms = 0
num_rows_error_cms = 0

start_time = time.time()

# Using try-except blocks to perform transactions and insert rows into the database
with conn.transaction():
    for column, row in cms.iterrows():
        try:
            with conn.transaction():
                
                # If a hospital already exists in the address table then add row to ratings table
                cur.execute("INSERT into ratings "
                            "(hospital_name, "
                            "hospital_pk, "
                            "collection_week, "
                            "overall_quality_rating, "
                            "type, "
                            "emergency_services_provided) "
                            "VALUES (%(hospital_name)s, "
                            "%(hospital_pk)s, "
                            "%(collection_week)s, "
                            "%(overall_quality_rating)s, "
                            "%(type)s, "
                            "%(emergency_services_provided)s)",
                            {'hospital_name' : str(row['Facility Name']),
                             'hospital_pk' : str(row['Facility ID']),
                             'collection_week' : str(row.collection_week),
                             'overall_quality_rating' : float(row['Hospital overall rating']),
                             'type' : str(row['Hospital Ownership']),
                             'emergency_services_provided' : row.emergency_services_provided})

        except Exception as e:
            row = dict(row)
            error_rows_cms = error_rows_cms.append(row, ignore_index = True)
            num_rows_error_cms += 1

        else:
            num_rows_successfully_inserted_cms += 1

# Committing the transaction
conn.commit()

end_time = time.time()

# Saving the data frame containing the error files to a separate CSV file
error_rows_cms.to_csv("Error rows in CMS data set.csv", index = False)

# Printing the summary output
print("Time taken:", round(((end_time - start_time) / 60), 2), "minutes")
print("Number of rows successfully inserted:", round(num_rows_successfully_inserted_cms / cms.shape[0] * 100, 2), "%")
print("Number of rows unable to be inserted due to errors:", round(num_rows_error_cms / cms.shape[0] * 100, 2), "%")

# Closing the SQL connection
conn.close()
