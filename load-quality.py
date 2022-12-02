# Importing libraries
import psycopg
import pandas as pd
import requests
import io
from credentials import username, password
import sys
from data_cleaning import data_cleaning_hginfo

# Data loading and cleaning
# date = sys.argv[1]
# hhs_file = sys.argv[2]
date = '2021-07-01'
cms_file = 'Hospital_General_Information-2021-07.csv'
file = str('/Users/arshmacbook/Desktop/36-614/Project/hospital_quality_files/' + cms_file)
cms = data_cleaning_hginfo(date, file)[:5]

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
    for column_name, row in cms_hospital_quality.iterrows():
        try:
            with conn.transaction():
                    
                # If a hospital already exists in the table then update existing values
                cur.execute("SELECT hospital_pk FROM address")
                existing_hospitals = pd.Series(cur.fetchall())
                if row.hospital_pk in existing_hospitals:
                    
                    # Update info in ratings table
                    cur.execute("UPDATE ratings "
                                "SET overall_quality_rating = %(overall_quality_rating)s, "
                                "type = %(type)s, "
                                "emergency_services_provided = %(emergency_services_provided)s "
                                "WHERE hospital_pk = %(hospital_pk)s",
                                {'overall_quality_rating' : float(row.overall_quality_rating),
                                 'type' : str(row.type),
                                 'emergency_services_provided' : bool(row.emergency_services_provided),
                                 'hospital_pk' : str(row.hospital_pk)})
                    
                # If the hospital does not exist in the table then insert it
                else:
                    
                    # Add row to address table
                    cur.execute("INSERT into address "
                                "(hospital_name, "
                                "hospital_pk, "
                                "address, "
                                "city, "
                                "state, "
                                "zip) "
                                "VALUES (%(hospital_name)s, "
                                "%(hospital_pk)s, "
                                "%(address)s, "
                                "%(city)s, "
                                "%(state)s, "
                                "%(zip)s)",
                                {'hospital_name' : str(row.hospital_name),
                                 'hospital_pk' : str(row.hospital_pk),
                                 'address': str(row.address),
                                 'city': str(row.city),
                                 'state': str(row.state),
                                 'zip': str(row.zip)})

                    # Add rows to capacity info table
                    cur.execute("INSERT into capacity_info "
                                "(hospital_pk) "
                                "VALUES (%(hospital_pk)s)",
                                {'hospital_pk' : str(row.hospital_pk)})

                    # Add rows to covid info table
                    cur.execute("INSERT into covid_info "
                                "(hospital_pk, "
                                "hospital_name) "
                                "VALUES (%(hospital_pk)s), "
                                "%(hospital_name)s)",
                                {'hospital_pk' : str(row.hospital_pk),
                                 'hospital_name' : str(row.hospital_name)})

                    # Add rows to ratings table
                    cur.execute("INSERT into ratings "
                                "(hospital_name, "
                                "hospital_pk, "
                                "overall_quality_rating, "
                                "type, "
                                "emergency_services_provided) "
                                "VALUES (%(hospital_name)s, "
                                "%(hospital_pk)s, "
                                "%(emergency_services_provided)s, "
                                "%(type)s, "
                                "%(emergency_services_provided)s)",
                                {'hospital_name' : str(row.hospital_name),
                                 'hospital_pk' : str(row.hospital_pk),
                                 'overall_quality_rating' : float(row.overall_quality_rating),
                                 'type' : str(row.type),
                                 'emergency_services_provided' : bool(row.emergency_services_provided)})

        except Exception as e:
            row = dict(row)
            error_rows_cms = error_rows_cms.append(row, ignore_index = True)
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
