# Importing libraries
import psycopg
import pandas as pd
import requests
import io
from credentials import username, password
import sys
from data_cleaning import data_cleaning_hginfo

# Data loading and cleaning
input_date = sys.argv[1]
cms_file = sys.argv[2]
file = str('/Users/arshmacbook/Desktop/36-614/Project/hospital_quality_files/' + cms_file) # Change this to user directory
cms = data_cleaning_hginfo(date, file)

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
                            "%(emergency_services_provided)s, "
                            "%(type)s, "
                            "%(emergency_services_provided)s)",
                            {'hospital_name' : str(row.hospital_name),
                             'hospital_pk' : str(row.hospital_pk),
                             'collection_week' : date(input_date),
                             'overall_quality_rating' : float(row.overall_quality_rating),
                             'type' : str(row.type),
                             'emergency_services_provided' : bool(row.emergency_services_provided)})

        except Exception as e:
            row = dict(row)
            error_rows_hhs = error_rows_hhs.append(row, ignore_index = True)
            num_rows_error_cms += 1

        else:
            num_rows_successfully_inserted_cms += 1

# Committing the transaction
conn.commit()

# Saving the data frame containing the error files to a separate CSV file
error_rows_cms.to_csv("Error rows in CMS data set.csv", index = False)

# Printing the summary output
print("Number of rows successfully inserted:", num_rows_successfully_inserted_cms)
print("Number of rows not inserted due to error:", num_rows_error_cms)

# Closing the SQL connection
conn.close()
