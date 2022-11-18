-- Group Name: Hounds
-- Members: Handi Yang, Ziyang Ren, Arsh Gupta

-- Parent table: Address
-- The address table is the parent table that stores the basic geographical information about each hospital in the HHS data set.
-- It has hospital_pk as its primary key, which refers to a unique ID for each hospital. This is also used as the reference to foreign key in various other tables.

create table address (
	hospital_name text not null,
	hospital_pk text primary key,
	address text,
	city varchar(255),
	state char(2),
	zip varchar(9),
	fips_code integer
);


-- Table: Capacity
-- The capacity_info table describe the capacity of the hospitals with information about the bed numbers for adult, pediatric inpatient, adult inpatient occupied, pediatric inpatient bed occupied, icu beds in total and used.
-- It uses facility ID as foreign key with Address as parent table. The table follows the schema in designing a database.
-- Each identity have a unique identifier, single value for each attribute and each non-key attribute only depends on the primary key.

create table capacity_info (
	hospital_pk text REFERENCES address (hospital_pk),
	adult_hospital_beds numeric CHECK(adult_hospital_beds >= 0),
	pediatric_inpatient_beds numeric CHECK (pediatric_inpatient_beds >= 0),
	adult_hospital_inpatient_bed_occupied numeric CHECK (adult_hospital_inpatient_bed_occupied >= 0),
	pediatric_inpatient_bed_occupied numeric CHECK (pediatric_inpatient_bed_occupied >= 0),
	total_icu_beds int CHECK (total_icu_beds >= 0),
	icu_beds_used int CHECK (icu_beds_used >= 0),
	CHECK (icu_beds_used <= total_icu_beds)
 );


-- Table: COVID_info
-- The COVID_info table describes the number of people getting covid in that hospital and hospital bed usage information related to covid.
-- This table references the hospital_pk in the address_info table as the primary key in COVID_info table.

Create table COVID_info (
	hospital_pk text REFERENCES address (hospital_pk),
	hospital_name text NOT NULL,
	inpatient_beds_used_covid_7_day_avg integer
	CHECK (inpatient_beds_used_covid_7_day_avg >= 0),
	staffed_adult_icu_patients_confirmed_covid_7_day_avg integer
	CHECK (staffed_adult_icu_patients_confirmed_covid_7_day_avg >= 0),
	total_icu_beds_7_day_avg integer
	CHECK (total_icu_beds_7_day_avg >= 0),
	icu_beds_used_7_day_avg integer
	CHECK (icu_beds_used_7_day_avg >= 0),	
	CHECK (inpatient_beds_used_covid_7_day_avg >= staffed_adult_icu_patients_confirmed_covid_7_day_avg)
);


-- Table: Ratings
-- The ratings table contains information about the rating for each hospital in the HHS data set.
-- In addition to containing information about the overall rating, it also contains information about the type of hospital and whether it offers emergency services or not since that can potentially have an influence on the overall rating.

create table ratings (	
	hospital_name text not null,
	hospital_pk text references address (hospital_pk),
	overall_quality_rating numeric check (overall_quality_rating >= 0),
	type varchar(255) check (type in ('Government - Hospital District or Authority',
	'Proprietary',
	'Voluntary non-profit - Private',
	'Government - State',
	'Voluntary non-profit - Other',
	'Government - Local',
	'Voluntary non-profit - Church',
	'Government - Federal',
	'Tribal',
	'Department of Defense',
	'Physician')),
	emergency_services_provided boolean default false
);