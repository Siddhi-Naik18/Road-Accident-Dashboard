import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import numpy as np
import urllib.parse

file_path = "DataSet_accident data.csv"
df = pd.read_csv(file_path)

print("Original Data:", df.shape)

# Remove unnecessary columns
columns_to_drop = ["Index", "Latitude", "Longitude","District Area"]
df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors = "ignore" )

# Add new Year column
df['Accident Date'] = pd.to_datetime(df['Accident Date'], errors='coerce', dayfirst=True)
df['Year'] = df['Accident Date'].dt.year

# Remove rows with specific years
df = df[~df['Year'].isin([2019,2022])]

# Split light_conditions by "-" and keep the only part before "-"
df['Light_Conditions'] = df['Light_Conditions'].str.split('-').str[0].str.strip()

# Replace values to standard values
df['Road_Surface_Conditions'] = df['Road_Surface_Conditions'].replace('Wet or damp', "Wet")
df['Road_Surface_Conditions'] = df['Road_Surface_Conditions'].replace('Frost or ice', "Ice")

# Fill blank values
df['Road_Type'] = df['Road_Type'].fillna("Unknown")
df['Urban_or_Rural_Area'] = df['Urban_or_Rural_Area'].fillna("Unallocated")
df['Weather_Conditions'] = df['Weather_Conditions'].fillna("Unknown")
df['Road_Surface_Conditions'] = df['Road_Surface_Conditions'].fillna("Unknown")

# Split the Weather Conditions column
df[['Weather_Conditions', 'Wind_Conditions']] = df['Weather_Conditions'].str.split('+', n=1, expand=True)

# Clean up whitespace
df['Weather_Conditions'] = df['Weather_Conditions'].str.strip()
df['Wind_Conditions'] = df['Wind_Conditions'].str.strip()

# Fill NaN values in Wind_Conditions with "Other"
df['Wind_Conditions'] = df['Wind_Conditions'].fillna("Other")

df['Vehicle_Type'] = df['Vehicle_Type'].str.lower()


# Standardize vehicle type values
df['Vehicle_Type'] = np.select(
    [
        df['Vehicle_Type'].str.contains("car", case=False, na=False),
        df['Vehicle_Type'].str.contains("bus", case=False, na=False),
        df['Vehicle_Type'].str.contains("motorcycle", case=False, na=False),
        df['Vehicle_Type'].str.contains("goods", case=False, na=False),
        df['Vehicle_Type'].str.contains("agricultural", case=False, na=False),
    ],
    [
        "Car",
        "Bus",
        "Bike",
        "Goods Van",
        "Agriculture"
    ],
    default="Other"
)

df['Year'] = pd.to_datetime(df['Year'].astype(str) + "-01-01")


# print(df.isnull().sum())
# print(df['Vehicle_Type'].tail(20))
# print(df.info())


# Database connection details
user = "root"          # your MySQL username
password = urllib.parse.quote_plus("NaikPC@58")# your MySQL password
host = "localhost"     # or 127.0.0.1
port = "3306"              # default MySQL port
database = "accidents_db"

# Create SQLAlchemy engine
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
)

# Load dataframe into MySQL (replace if exists)
df.to_sql("accidents", con=engine, if_exists="replace", index=False)

print("✅ Data successfully loaded into MySQL table 'accidents'")

