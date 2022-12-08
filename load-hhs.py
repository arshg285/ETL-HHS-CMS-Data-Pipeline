import psycopg
import pandas as pd
import time
import credentials as cd
import numpy as np
import sys
from data_cleaning import data_cleaning_hhs
import warnings
warnings.filterwarnings('ignore')

# Data loading and cleaning
hhs_file = sys.argv[1]
file = str('/Users/arshmacbook/Desktop/36-614/data_engineering_project/hhs_weekly_data_files/' + hhs_file) # Change this to user directory
hhs = data_cleaning_hhs(file)

conn = psycopg.connect(
    host = "sculptor.stat.cmu.edu",
    dbname = cd., # Insert your dbname
    user = cd., # Insert your username
    password = cd. # Insert your password
)

# Creating a cursor object
cur = conn.cursor()

error_rows_hhs = pd.DataFrame()
num_rows_successfully_inserted_hhs = 0
num_rows_error_hhs = 0

start_time = time.time()

# Using try-except blocks to perform transactions and insert rows into the database
with conn.transaction():
    for column, row in hhs.iterrows():
        try:
            with conn.transaction():
                
                # If a hospital already exists in the table then update values in the address table
                cur.execute("SELECT hospital_pk FROM address")
                existing_hospitals = np.array([elem[0] for elem in pd.Series(cur.fetchall())])
                if row.hospital_pk in existing_hospitals:

                    # Update info in address table
                    cur.execute("UPDATE address "
                                 "SET hospital_name = %(hospital_name)s, "
                                 "address = %(address)s, "
                                 "city = %(city)s, "
                                 "state = %(state)s, "
                                 "zip = %(zip)s, "
                                 "fips_code = %(fips_code)s"
                                 "WHERE hospital_pk = %(hospital_pk)s",
                                 {'hospital_name' : str(row.hospital_name),
                                  'hospital_pk' : str(row.hospital_pk),
                                  'address' : str(row.address),
                                  'city' : str(row.city),
                                  'state' : str(row.state),
                                  'zip' : str(row.zip),
                                  'fips_code' : float(row.fips_code)})

                # If the hospital does not exist in the address table then insert it
                else:

                    # Add row to address table
                    cur.execute("INSERT into address "
                                 "(hospital_name, "
                                 "hospital_pk, "
                                 "address, "
                                 "city, "
                                 "state, "
                                 "zip, "
                                 "fips_code) "
                                 "VALUES (%(hospital_name)s, "
                                 "%(hospital_pk)s, "
                                 "%(address)s, "
                                 "%(city)s, "
                                 "%(state)s, "
                                 "%(zip)s, "
                                 "%(fips_code)s)",
                                 {'hospital_name' : str(row.hospital_name),
                                  'hospital_pk' : str(row.hospital_pk),
                                  'address' : str(row.address),
                                  'city' : str(row.city),
                                  'state' : str(row.state),
                                  'zip' : str(row.zip),
                                  'fips_code' : float(row.fips_code)})

                # Add rows to capacity info table
                cur.execute("INSERT into capacity_info "
                            "(hospital_pk, "
                            "collection_week, "
                            "adult_hospital_beds, "
                            "pediatric_inpatient_beds, "
                            "adult_hospital_inpatient_bed_occupied, "
                            "pediatric_inpatient_bed_occupied, "
                            "total_icu_beds, "
                            "icu_beds_used) "
                            "VALUES (%(hospital_pk)s, "
                            "%(collection_week)s, "
                            "%(adult_hospital_beds)s, "
                            "%(pediatric_inpatient_beds)s, "
                            "%(adult_hospital_inpatient_bed_occupied)s, "
                            "%(pediatric_inpatient_bed_occupied)s, "
                            "%(total_icu_beds)s, "
                            "%(icu_beds_used)s)",
                            {'hospital_pk' : str(row.hospital_pk),
                             'collection_week' : str(row.collection_week),
                             'adult_hospital_beds' : float(row.all_adult_hospital_beds_7_day_avg),
                             'pediatric_inpatient_beds' : float(row.all_pediatric_inpatient_beds_7_day_avg),
                             'adult_hospital_inpatient_bed_occupied' : float(row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage),
                             'pediatric_inpatient_bed_occupied' : float(row.all_pediatric_inpatient_bed_occupied_7_day_avg),
                             'total_icu_beds' : float(row.total_icu_beds_7_day_avg),
                             'icu_beds_used' : float(row.icu_beds_used_7_day_avg)})

                # Add rows to covid info table
                cur.execute("INSERT into covid_info "
                            "(hospital_pk, "
                            "hospital_name, "
                            "collection_week, "
                            "inpatient_beds_used_covid_7_day_avg, "
                            "staffed_adult_icu_patients_confirmed_covid_7_day_avg, "
                            "total_icu_beds_7_day_avg, "
                            "icu_beds_used_7_day_avg) "
                            "VALUES (%(hospital_pk)s, "
                            "%(hospital_name)s, "
                            "%(collection_week)s, "
                            "%(inpatient_beds_used_covid_7_day_avg)s, "
                            "%(staffed_adult_icu_patients_confirmed_covid_7_day_avg)s, "
                            "%(total_icu_beds_7_day_avg)s, "
                            "%(icu_beds_used_7_day_avg)s)",
                            {'hospital_pk' : str(row.hospital_pk),
                             'hospital_name' : str(row.hospital_name),
                             'collection_week' : str(row.collection_week),
                             'inpatient_beds_used_covid_7_day_avg' : float(row.inpatient_beds_used_covid_7_day_avg),
                             'staffed_adult_icu_patients_confirmed_covid_7_day_avg' : float(row.staffed_icu_adult_patients_confirmed_covid_7_day_avg),
                             'total_icu_beds_7_day_avg' : float(row.total_icu_beds_7_day_avg),
                             'icu_beds_used_7_day_avg' : float(row.icu_beds_used_7_day_avg)})

        except Exception as e:
            row = dict(row)
            error_rows_hhs = error_rows_hhs.append(row, ignore_index = True)
            num_rows_error_hhs += 1

        else:
            num_rows_successfully_inserted_hhs += 1

# Committing the transaction
conn.commit()

end_time = time.time()

# Saving the data frame containing the error files to a separate CSV file
error_rows_hhs.to_csv("Error rows in HHS data set.csv", index = False)

# Printing the summary output
print("\nTime taken:", round(((end_time - start_time) / 60), 2), "minutes\n")
print("Number of rows successfully inserted:", round(num_rows_successfully_inserted_hhs / hhs.shape[0] * 100, 2), "%\n")
print("Number of rows unable to be inserted due to errors:", round(num_rows_error_hhs / hhs.shape[0] * 100, 2), "%\n")

# Closing the SQL connection
conn.close()
