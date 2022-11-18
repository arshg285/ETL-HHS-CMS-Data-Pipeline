# Importing libraries
import psycopg
import pandas as pd
import requests
import io
from credentials import username, password
import sys
from data_cleaning import data_cleaning_hginfo

cms = data_cleaning_hhs('/Users/zoe/Desktop/Data engineering/Project/data_engineering_project/hhs_weekly_data_files/' + sys.argv[2])

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
    user = %(username)s,
    password = %(password)s,
    params = {'username' : username,
              'password' : password}
)

# Creating a cursor object
cur = conn.cursor()

# Getting all hospital IDs to compare columns with
cur.execute("SELECT hospital_pk FROM address")
existing_hospitals = pd.Series(cur.fetchall())

error_rows_cms = pd.DataFrame()
num_rows_successfully_inserted_cms = 0
num_rows_error_cms = 0

## Using try-except blocks to perform transactions and insert rows into the database
with conn.transaction():
    for column_name, row in cms_hospital_quality.iterrows():
        
        # If the current hospital is not already in the table, we add it
        if `row.Facility ID` not in existing_hospitals:
        
            # Make a new savepoint: Adding values to address table
            try:
                with conn.transaction():
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
                                params = {'hospital_name' : `row.Facility Name`,
                                          'hospital_pk' : `row.Facility ID`,
                                          'address' : row.Address,
                                          'city' : row.City,
                                          'state' : row.State,
                                          'zip' : `row.ZIP Code`})

            except Exception as e:
                error_rows_cms.append(e)
                num_rows_error_cms += 1
                print("Insert failed")

            else:
                num_rows_successfully_inserted_cms += 1

            # Make a new savepoint: Adding values to capacity_info table
            try:
                with conn.transaction():
                    cur.execute("INSERT into capacity_info "
                                "(hospital_pk) "
                                "VALUES (%(hospital_pk)s)",
                                params = {'hospital_pk' : `row.Facility ID`})

            except Exception as e:
                error_rows_cms.append(e)
                num_rows_error_cms += 1
                print("Insert failed")

            else:
                num_rows_successfully_inserted_cms += 1

            # Make a new savepoint: Adding values to covid_info table
            try:
                with conn.transaction():
                    cur.execute("INSERT into covid_info "
                                "(hospital_pk, "
                                "hospital_name) "
                                "VALUES (%(hospital_pk)s), "
                                "%(hospital_name)s)",
                                params = {'hospital_pk' : `row.Facility Name`,
                                          'hospital_name' : `row.Facility ID`})

            except Exception as e:
                error_rows_cms.append(e)
                num_rows_error_cms += 1
                print("Insert failed")

            else:
                num_rows_successfully_inserted_cms += 1

            # Make a new savepoint: Adding values to ratings table
            try:
                with conn.transaction():
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
                                params = {'hospital_name' : `row.Facility Name`,
                                          'hospital_pk' : `row.Facility ID`,
                                          'overall_quality_rating' : `row.Hospital overall rating`,
                                          'type' : `row.Hospital Ownership`,
                                          'emergency_services_provided' : `row.Emergency Services`})

            except Exception as e:
                error_rows_cms.append(e)
                num_rows_error_cms += 1
                print("Insert failed")

            else:
                num_rows_successfully_inserted_cms += 1
                
        # If the current hospital is already in the table, we update the ratings table
        if `row.Facility ID` in existing_hospitals:
            
            # Make a new savepoint: Adding values to ratings table
            try:
                with conn.transaction():
                    cur.execute("UPDATE ratings "
                                "SET overall_quality_rating = %(overall_quality_rating)s, "
                                "type = %(type)s, "
                                "emergency_services_provided = %(emergency_services_provided)s "
                                "WHERE hospital_pk = %(hospital_pk)s",
                               params = {'overall_quality_rating' : `row.Hospital overall rating`,
                                         'type' : `row.Hospital Ownership`,
                                         'emergency_services_provided' `row.Emergency Services`: ,
                                         'hospital_pk' : `row.Facility ID`})

            except Exception as e:
                error_rows_cms.append(e)
                num_rows_error_cms += 1
                print("Insert failed")

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
