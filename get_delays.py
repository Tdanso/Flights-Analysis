
import psycopg2
import pandas as pd
import numpy as np 
from config import params

conn = psycopg2.connect(**params)


cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE False = cancelled and False = diverted; ")
rows = cursor.fetchall()

cursor.close()


df = pd.DataFrame(rows, columns=[desc.name for desc in cursor.description])

#check for null values 
print(df.isna().sum())

print(df.head())

df["delayed"] = np.where(((df["arr_del15"] == True) | (df["dep_del15"] == True)), 1, 0)

# group by airline and compute the average rationof delays 
grouped_airline = df.groupby("op_unique_carrier")["delayed"].mean() 

print(grouped_airline)

# df is sorted by delays
sorted_airline = grouped_airline.sort_values()


# to save as csv
sorted_airline.to_csv("airline_delays.csv")


# group by airline and compute the average rationof delays 
grouped_airport = df.groupby("origin")["delayed"].mean()

# df is sorted by delays
sorted_airport = grouped_airport.sort_values()


# to save as csv
sorted_airport.to_csv("airport_delays.csv")