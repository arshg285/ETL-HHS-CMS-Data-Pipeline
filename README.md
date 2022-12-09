We have four modules in this GitHub repository: `data_cleaning`, `load_hhs.py` , `load_quality.py` and `weekly_report.py`

The module `data_cleaning.py` idneify all type of missing value as 'NA' and make some change for data format. We do not need to run it individually, it runs inside `load_hhs.py` and `load_quality.py`

The module `load_hhs.py` taking in the HHS data file as the input, performs data wrangling, and inserts rows from the CSV file into address table, capacity_info table and covid_info table as specified by `schema.sql`. You can run this code using: python load_hhs.py [file_name]

The `load_quality.py` module takes in the CMS quality files, perferms data wrangling, and insert rows into quality table as specified by `schema.sql`. You can run this code using: python load_quality.py [collection_date] [file_name]

The model `weekly_report.py` well generate report about the following questions based on current data:
1. A summary of how many hospital records were loaded in the most recent week, and how that compares to previous weeks.
2. A table summarizing the number of adult and pediatric beds available this week, the number used, and the number used by patients with COVID, compared to the 4 most recent weeks
3. A graph summarizing the fraction of beds currently in use by hospital quality rating, so we can compare high-quality and low-quality hospitals
4. A plot of the total number of hospital beds used per week, over all time, split into all cases and COVID cases
5. Map?
6. A map showing state with their COVID cases, which state has the lowest cases and which state has the highest cases
7. A map showing states with their average hospital rating of that state
You can run this code using: streamlit run weekly_report.py

