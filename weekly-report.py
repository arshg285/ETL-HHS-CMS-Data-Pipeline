import psycopg
import pandas as pd
import credentials as cd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import warnings
from fpdf import FPDF
import streamlit as st
warnings.filterwarnings('ignore')

st.title('HHS and CMS Data Summary')

# Establishing SQL connection
conn = psycopg.connect(
    host = "sculptor.stat.cmu.edu",
    dbname = cd.,  # Insert your dbname
    user = cd.,  # Insert your username
    password = cd.  # Insert your password
)

# Creating a cursor object
cur = conn.cursor()


# Required Questions

# Question 1
st.header("Summary 1")
st.write("A summary of how many hospital records were loaded in the most recent week, and how that compares to previous weeks.")

# SQL query

# Question 2
st.header("Summary 2")
st.write("A table summarizing the number of adult and pediatric beds available this week, the number used, and the number used by patients with COVID, compared to the 4 most recent weeks")

# SQL query

# Question 3
st.header("Summary 3")
st.write("A graph or table summarizing the fraction of beds currently in use by hospital quality rating, so we can compare high-quality and low-quality hospitals")

# SQL query
sql_query_1 = "select ratings.hospital_name, \
    avg(coalesce(nullif(ratings.overall_quality_rating,'NaN'), 0)) as rating, \
    avg((coalesce(nullif(adult_hospital_inpatient_bed_occupied, 'NaN'), 0)) / adult_hospital_beds) as adult_occupied, \
    avg((coalesce(nullif(pediatric_inpatient_bed_occupied, 'NaN'), 0)) / pediatric_inpatient_beds) as child_occupied \
    from capacity_info \
    inner join ratings \
    on capacity_info.hospital_pk = ratings.hospital_pk \
    where capacity_info.adult_hospital_beds > 0 \
    and capacity_info.pediatric_inpatient_beds > 0 \
    group by ratings.hospital_name \
    order by rating desc"

sql_query_2 = "select ratings.hospital_name, \
    avg(coalesce(nullif(ratings.overall_quality_rating,'NaN'), 0)) as rating, \
    avg((coalesce(nullif(adult_hospital_inpatient_bed_occupied, 'NaN'),0)) / adult_hospital_beds) as adult_occupied, \
    avg((coalesce(nullif(pediatric_inpatient_bed_occupied, 'NaN'), 0)) / pediatric_inpatient_beds) as child_occupied \
    from capacity_info \
    inner join ratings \
    on capacity_info.hospital_pk = ratings.hospital_pk \
    where capacity_info.adult_hospital_beds > 0 \
    and capacity_info.pediatric_inpatient_beds > 0 \
    group by ratings.hospital_name \
    order by rating asc"

df_sql_query_1 = pd.read_sql_query(sql_query_1, conn).dropna().head(n = 10)
df_sql_query_2 = pd.read_sql_query(sql_query_2, conn).dropna().head(n = 10)

st.dataframe(df_sql_query_1)
st.dataframe(df_sql_query_2)

# Generating plots
n = df_sql_query_1.hospital_name.shape[0]
ind = np.arange(n)
width = 0.25

plt.bar(
    ind,
    df_sql_query_1.adult_occupied,
    width,
    label = 'Adult',
    )

plt.bar(
    ind + width,
    df_sql_query_1.child_occupied,
    width,
    label = 'Child',
    )

plt.xticks(
    ind + width / 2,
    df_sql_query_1.hospital_name,
    rotation = 90,
    fontsize = 6
    )

plt.xlabel('Hospitals')
plt.ylabel('Proportion of beds occupied')
plt.title('Bed Usage Information For Top 10 Rated Hospitals')
plt.legend()
plt.tight_layout()
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_3.1.png")
plt.close()
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_3.1.png")

plt.bar(
    ind,
    df_sql_query_2.adult_occupied,
    width,
    label = 'Adult',
    )

plt.bar(
    ind + width,
    df_sql_query_2.child_occupied,
    width,
    label = 'Child',
    )

plt.xticks(
    ind + width / 2,
    df_sql_query_2.hospital_name,
    rotation = 90,
    fontsize = 6
    )

plt.xlabel('Hospitals')
plt.ylabel('Proportion of beds occupied')
plt.title('Bed Usage Information For Lowest 10 Rated Hospitals')
plt.legend()
plt.tight_layout()
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_3.2.png")
plt.close()
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_3.2.png")

# Question 4
st.header("Summary 4")
st.write("A plot of the total number of hospital beds used per week, over all time, split into all cases and COVID cases")

# SQL query
sql_query = "select all_beds.collection_week as collection_week, \
    all_beds.all_beds_used as all_beds_used, \
    covid_beds.covid_beds_used as covid_beds_used \
    from ( \
        select temp.collection_week, \
        sum(temp.all_beds_used) as all_beds_used \
        from ( \
            select collection_week, ( \
            coalesce(nullif(adult_hospital_inpatient_bed_occupied, 'NaN'), 0) + \
            coalesce(nullif(pediatric_inpatient_bed_occupied,'NaN'), 0) \
            ) as all_beds_used \
            from capacity_info \
            ) as temp \
        group by temp.collection_week \
        ) as all_beds \
    inner join ( \
        select temp.collection_week, \
        sum(temp.covid_beds_used) as covid_beds_used \
        from ( \
            select collection_week, \
            coalesce(nullif(inpatient_beds_used_covid_7_day_avg, 'NaN'), 0) as covid_beds_used \
            from covid_info \
            ) as temp \
        group by temp.collection_week \
        ) as covid_beds \
    on all_beds.collection_week = covid_beds.collection_week"

covid_and_all_beds_used_per_week = pd.read_sql_query(sql_query, conn)

st.dataframe(covid_and_all_beds_used_per_week)

# Generating plot
n = covid_and_all_beds_used_per_week.collection_week.shape[0]
ind = np.arange(n)
width = 0.25

plt.bar(
    ind,
    covid_and_all_beds_used_per_week.all_beds_used,
    width,
    label = "All Cases"
    )

plt.bar(
    ind + width,
    covid_and_all_beds_used_per_week.covid_beds_used,
    width,
    label = "COVID Cases"
    )

plt.xticks(
    ind + width / 2,
    covid_and_all_beds_used_per_week.collection_week,
    rotation = 45
    )

plt.xlabel("Collection Week")
plt.ylabel("Number of beds used")
plt.title("Number of beds used over time for all cases and COVID cases")
plt.legend()
plt.tight_layout()
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_4.png")
plt.close()
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_4.png")


# Additional Questions

# Question 1
st.header("Summary 5")
st.write("")

# SQL query

# Question 2
st.header("Summary 6")
st.write("A map showing average rating of hospitals for each state.")

# SQL query
sql_query = "select address.state, \
    avg(coalesce(nullif(ratings.overall_quality_rating, 'NaN'), 0)) as rating \
    from ratings \
    inner join address \
    on address.hospital_pk = ratings.hospital_pk \
    group by address.state"

df = pd.read_sql_query(sql_query, conn)

# Generating plot
fig = px.choropleth(df,
                    locations = 'state',
                    locationmode = "USA-states",
                    scope = "usa",
                    color = 'rating',
                    color_continuous_scale = "Viridis_r"
                    )

fig.update_layout(
      title_text = 'Average Hospital Rating of Each State in US',
      title_x = 0.5
      )

fig.write_image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_6.png")
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_6.png")

# Question 3
st.header("Summary 7")
st.write("A map showing number of COVID cases for each state.")

# SQL query
sql_query = "select address.state as state, \
    sum(covid_info.covid_cases) as covid_cases \
    from ( \
        select hospital_pk, \
        coalesce(nullif(inpatient_beds_used_covid_7_day_avg, 'NaN'), 0) as covid_cases \
        from covid_info \
        ) as covid_info \
    left join ( \
        select hospital_pk, \
        state \
        from address \
        ) as address \
    on address.hospital_pk = covid_info.hospital_pk \
    group by state \
    order by covid_cases desc"
covid_cases_per_state = pd.read_sql_query(sql_query, conn)

st.write("Top 10 states with highest covid cases")
st.dataframe(covid_cases_per_state.head(n = 10))

# Generating plot
covid_cases_map = px.choropleth(
      covid_cases_per_state,
      locations = covid_cases_per_state.state,
      locationmode = "USA-states",
      scope = "usa",
      color = np.log(covid_cases_per_state.covid_cases),
      color_continuous_scale = "Viridis_r"
      )

covid_cases_map.update_layout(
      title_text = 'COVID Cases by State (on log scale)',
      title_x = 0.5
      )

covid_cases_map.write_image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_7.png")
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/summary_7.png")

# Closing the SQL connection
conn.close()
