import psycopg
import pandas as pd
import credentials as cd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import warnings
import streamlit as st
warnings.filterwarnings('ignore')

st.title('HHS and CMS Data Summary')
st.subheader("Team members: Ziyang Ren, Handi Yang, Arsh Gupta")

st.write('This report provides a summary of the HHS hospitals and CMS quality \
    data over the past 1.5 years.')

# Establishing SQL connection
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu",
    dbname=cd.arsh_dbname,  # Insert your dbname
    user=cd.arsh_username,  # Insert your username
    password=cd.arsh_password  # Insert your password
)

# Creating a cursor object
cur = conn.cursor()


# Required Questions

# Question 1
# Summary records compared to previous weeks
st.header('Summary 1: Comparison of hospital records over time')
st.write("The tables below show how many hospital records were loaded in the \
    most recent week, and how that compares to previous weeks.")

# SQL Query
sql_query = "select collection_week, \
    count, \
    count-LAG(count) over (order by collection_week) as changes \
    from ( \
        select collection_week, \
        count(*) as count \
        from capacity_info \
        group by collection_week \
        ) AS weekly_record \
    order by collection_week desc"

hhs_summary = pd.read_sql_query(sql_query, conn)

sql_query = "select collection_week, \
    count, \
    count-LAG(count) over (order by collection_week) as changes \
    from (select collection_week, count(*) as count \
    from ratings \
    group by collection_week) AS weekly_record \
    order by collection_week desc"

cms_summary = pd.read_sql_query(sql_query, conn)

st.subheader("Changes in weekly entries for HHS data")
st.table(hhs_summary)

st.subheader("Changes in weekly entries for CMS quality data")
st.table(cms_summary)

# Question 2
# Summary records compared to previous 4 weeks
st.header('Summary 2: Adult and pediatric beds availability and usage for \
    COVID and non-COVID patients over time')
st.write("The table and graph below summarize the number of adult and \
    pediatric beds available this week, the number used, and the number used \
        by patients with COVID, compared to the 4 most recent weeks")

# SQL Query
sql_query = "select collection_week, \
    adult_available, \
    pediatric_available, \
    bed_used, \
    covid_used, \
    adult_available-LAG(adult_available) over (order by collection_week) \
        as change_adult_available, \
    pediatric_available-LAG(pediatric_available) over \
        (order by collection_week) as change_pediatric_available, \
    bed_used-LAG(bed_used) over (order by collection_week) \
        as change_bed_used, \
    covid_used-LAG(covid_used) over (order by collection_week) \
        as change_covid_used \
    from ( \
        select ca.collection_week as collection_week, \
        sum(coalesce(nullif(ca.adult_hospital_beds,'NaN'), 0) - \
            coalesce(nullif(ca.adult_hospital_inpatient_bed_occupied ,'NaN'), \
                0)) as adult_available, \
        sum(coalesce(nullif(ca.pediatric_inpatient_beds,'NaN'), 0) - \
            coalesce(nullif(ca.pediatric_inpatient_bed_occupied,'NaN'), 0) ) \
                as pediatric_available, \
        sum(coalesce(nullif(ca.icu_beds_used,'NaN'), 0) + \
            coalesce(nullif(ca.adult_hospital_inpatient_bed_occupied, 'NaN'), \
                0) + coalesce(nullif(ca.pediatric_inpatient_bed_occupied, \
                    'NaN'), 0)) as bed_used, \
        sum(coalesce(nullif(co.inpatient_beds_used_covid_7_day_avg,'NaN'), \
            0)) as covid_used \
        from capacity_info ca \
        join covid_info co on ca.hospital_pk = co.hospital_pk \
        group by ca.collection_week) AS weekly_record \
    order by collection_week desc \
    limit 5"

beds_summary = pd.read_sql_query(sql_query, conn)

st.table(beds_summary)
st.bar_chart(beds_summary[[
    'adult_available', 'pediatric_available', 'bed_used', 'covid_used'
    ]])
st.write("In the barplot, the x-axis is corresponding to the index of \
    collection week as in the summary table.")

# Question 3
st.header("Summary 3: Bed usage across high and low rated hospitals")
st.subheader("High rated hospitals")
st.write("The table and graph below summarize the fraction of beds currently \
    in use by hospitals with high quality rating.")

# SQL query
sql_query_1 = "select ratings.hospital_name, \
    avg(coalesce(nullif(ratings.overall_quality_rating,'NaN'), 0)) as rating, \
    avg((coalesce(nullif(adult_hospital_inpatient_bed_occupied, 'NaN'), 0)) / \
        adult_hospital_beds) as adult_occupied, \
    avg((coalesce(nullif(pediatric_inpatient_bed_occupied, 'NaN'), 0)) / \
        pediatric_inpatient_beds) as child_occupied \
    from capacity_info \
    inner join ratings \
    on capacity_info.hospital_pk = ratings.hospital_pk \
    where capacity_info.adult_hospital_beds > 0 \
    and capacity_info.pediatric_inpatient_beds > 0 \
    group by ratings.hospital_name \
    order by rating desc"

df_sql_query_1 = pd.read_sql_query(sql_query_1, conn).dropna().head(n=10)

# Generating plots
n = df_sql_query_1.hospital_name.shape[0]
ind = np.arange(n)
width = 0.25

plt.bar(
    ind,
    df_sql_query_1.adult_occupied,
    width,
    label='Adult',
    )

plt.bar(
    ind + width,
    df_sql_query_1.child_occupied,
    width,
    label='Child',
    )

plt.xticks(
    ind + width / 2,
    df_sql_query_1.hospital_name,
    rotation=90,
    fontsize=6
    )

plt.xlabel('Hospitals')
plt.ylabel('Proportion of beds occupied')
plt.title('Bed Usage Information For Top 10 Rated Hospitals')
plt.legend()
plt.tight_layout()
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_3.1.png")
plt.close()

st.dataframe(df_sql_query_1)
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_3.1.png")

st.subheader("Low rated hospital")
st.write("The table and graph below summarize the fraction of beds currently \
    in use by hospitals with low quality rating.")

sql_query_2 = "select ratings.hospital_name, \
    avg(coalesce(nullif(ratings.overall_quality_rating,'NaN'), 0)) as rating, \
    avg((coalesce(nullif(adult_hospital_inpatient_bed_occupied, 'NaN'),0)) / \
        adult_hospital_beds) as adult_occupied, \
    avg((coalesce(nullif(pediatric_inpatient_bed_occupied, 'NaN'), 0)) / \
        pediatric_inpatient_beds) as child_occupied \
    from capacity_info \
    inner join ratings \
    on capacity_info.hospital_pk = ratings.hospital_pk \
    where capacity_info.adult_hospital_beds > 0 \
    and capacity_info.pediatric_inpatient_beds > 0 \
    group by ratings.hospital_name \
    order by rating asc"

df_sql_query_2 = pd.read_sql_query(sql_query_2, conn).dropna().head(n=10)

plt.bar(
    ind,
    df_sql_query_2.adult_occupied,
    width,
    label='Adult',
    )

plt.bar(
    ind + width,
    df_sql_query_2.child_occupied,
    width,
    label='Child',
    )

plt.xticks(
    ind + width / 2,
    df_sql_query_2.hospital_name,
    rotation=90,
    fontsize=6
    )

plt.xlabel('Hospitals')
plt.ylabel('Proportion of beds occupied')
plt.title('Bed Usage Information For Lowest 10 Rated Hospitals')
plt.legend()
plt.tight_layout()
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_3.2.png")
plt.close()

st.dataframe(df_sql_query_2)
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_3.2.png")

# Question 4
st.header("Summary 4: Bed usage for COVID and non-COVID cases across time")
st.write("The table and plot below summarize the total number of hospital beds \
    used per week, over all time, split into all cases and COVID cases.")

# SQL query
sql_query = "select all_beds.collection_week as collection_week, \
    all_beds.all_beds_used as all_beds_used, \
    covid_beds.covid_beds_used as covid_beds_used \
    from ( \
        select temp.collection_week, \
        sum(temp.all_beds_used) as all_beds_used \
        from ( \
            select collection_week, ( \
            coalesce(nullif(adult_hospital_inpatient_bed_occupied, \
                'NaN'), 0) + \
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
            coalesce(nullif(inpatient_beds_used_covid_7_day_avg, 'NaN'), 0) \
                as covid_beds_used \
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
    label="All Cases"
    )

plt.bar(
    ind + width,
    covid_and_all_beds_used_per_week.covid_beds_used,
    width,
    label="COVID Cases"
    )

plt.xticks(
    ind + width / 2,
    covid_and_all_beds_used_per_week.collection_week,
    rotation=45
    )

plt.xlabel("Collection Week")
plt.ylabel("Number of beds used")
plt.title("Number of beds used over time for all cases and COVID cases")
plt.legend()
plt.tight_layout()
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_4.png")
plt.close()
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_4.png")


# Additional Questions

# Question 1
st.header('Summary 5: COVID bed usage across hospital types')
st.write("The table and pie chart below shows the number of beds used by COVID \
    patients at hospitals across different types.")

# SQL query
sql_query = "select sum(coalesce(nullif(co.inpatient_beds_used_covid_7_day_avg, \
    'NaN'), 0) ), \
    ra.type \
    from ratings ra \
    join ( \
        select hospital_pk, \
        collection_week, \
        inpatient_beds_used_covid_7_day_avg \
        from covid_info \
        where collection_week = '2022-10-21' \
        ) co \
    on co.hospital_pk = ra.hospital_pk \
    group by ra.type"

covid_type = pd.read_sql_query(sql_query, conn)

patches, l_text = plt.pie(
    covid_type[
        'sum'
        ].values.tolist(), labels=covid_type[
            'type'
            ].values.tolist()
    )
for t in l_text:
    t.set_size(4)

plt.title("Covid bed used in hospital types")
plt.savefig("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_5.png")
plt.close()

# Pie-chart for bed used in different type of hospitals
st.table(covid_type)
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_5.png")

# Question 2
st.header("Summary 6: Average rating of hospitals across the country")

# SQL query
sql_query = "select address.state, \
    avg(coalesce(nullif(ratings.overall_quality_rating, 'NaN'), 0)) as rating \
    from ratings \
    inner join address \
    on address.hospital_pk = ratings.hospital_pk \
    group by address.state \
    order by rating desc"

df = pd.read_sql_query(sql_query, conn)

# Generating plot
fig = px.choropleth(df,
                    locations='state',
                    locationmode="USA-states",
                    scope="usa",
                    color='rating',
                    color_continuous_scale="Viridis_r"
                    )

fig.update_layout(
      title_text='Average Hospital Rating of Each State in US',
      title_x=0.5
      )

fig.write_image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/\
    Plots/summary_6.png")

st.write("The table below shows the top 5 states with highest average hospital \
    ratings ranked in order.")
st.dataframe(df.head(n=5))

st.write("The map below shows the average rating of hospitals for each state.")
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_6.png")

# Question 3
st.header("Summary 7: COVID cases across the country")

# SQL query
sql_query = "select address.state as state, \
    sum(covid_info.covid_cases) as covid_cases \
    from ( \
        select hospital_pk, \
        coalesce(nullif(inpatient_beds_used_covid_7_day_avg, 'NaN'), 0) \
            as covid_cases \
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

# Generating plot
covid_cases_map = px.choropleth(
      covid_cases_per_state,
      locations=covid_cases_per_state.state,
      locationmode="USA-states",
      scope="usa",
      color=np.log(covid_cases_per_state.covid_cases),
      color_continuous_scale="Viridis_r"
      )

covid_cases_map.update_layout(
      title_text='COVID Cases by State (on log scale)',
      title_x=0.5
      )

covid_cases_map.write_image("/Users/arshmacbook/Desktop/36-614/\
    data_engineering_project/Plots/summary_7.png")

st.write("The table below shows the top 5 states with maximum COVID cases \
    ranked in order.")
st.dataframe(covid_cases_per_state.head(n=5))

st.write("The map below shows the number of COVID cases for each state.")
st.image("/Users/arshmacbook/Desktop/36-614/data_engineering_project/Plots/\
    summary_7.png")

# Closing the SQL connection
conn.close()
