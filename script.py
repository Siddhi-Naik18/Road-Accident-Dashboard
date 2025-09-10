import os
import pandas as pd
import numpy as np
import urllib.parse
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ------------------------------
# Load environment variables
# ------------------------------
load_dotenv()

user = os.getenv("MYSQL_USER")
password = urllib.parse.quote_plus(os.getenv("MYSQL_PASSWORD"))
host = os.getenv("MYSQL_HOST", "localhost")
port = os.getenv("MYSQL_PORT", "3306")
database = os.getenv("MYSQL_DATABASE")

# ------------------------------
# Load and clean CSV
# ------------------------------
file_path = "DataSet_accident data.csv"
df = pd.read_csv(file_path)

print("Original Data:", df.shape)

# Remove unnecessary columns
columns_to_drop = ["Index", "Latitude", "Longitude", "District Area"]
df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors="ignore")

# Add new Year column
df['Accident Date'] = pd.to_datetime(df['Accident Date'], errors='coerce', dayfirst=True)
df['Year'] = df['Accident Date'].dt.year

# Remove rows with specific years
df = df[~df['Year'].isin([2019, 2022])]

# Clean Light_Conditions
df['Light_Conditions'] = df['Light_Conditions'].str.split('-').str[0].str.strip()

# Standardize Road_Surface_Conditions
df['Road_Surface_Conditions'] = df['Road_Surface_Conditions'].replace({
    "Wet or damp": "Wet",
    "Frost or ice": "Ice"
})

# Fill blank values
df['Road_Type'] = df['Road_Type'].fillna("Unknown")
df['Urban_or_Rural_Area'] = df['Urban_or_Rural_Area'].fillna("Unallocated")
df['Weather_Conditions'] = df['Weather_Conditions'].fillna("Unknown")
df['Road_Surface_Conditions'] = df['Road_Surface_Conditions'].fillna("Unknown")

# Split Weather_Conditions into Weather + Wind
df[['Weather_Conditions', 'Wind_Conditions']] = df['Weather_Conditions'].str.split('+', n=1, expand=True)
df['Weather_Conditions'] = df['Weather_Conditions'].str.strip()
df['Wind_Conditions'] = df['Wind_Conditions'].str.strip().fillna("Other")

# Standardize vehicle type
df['Vehicle_Type'] = df['Vehicle_Type'].str.lower()
df['Vehicle_Type'] = np.select(
    [
        df['Vehicle_Type'].str.contains("car", case=False, na=False),
        df['Vehicle_Type'].str.contains("bus", case=False, na=False),
        df['Vehicle_Type'].str.contains("motorcycle", case=False, na=False),
        df['Vehicle_Type'].str.contains("goods", case=False, na=False),
        df['Vehicle_Type'].str.contains("agricultural", case=False, na=False),
    ],
    ["Car", "Bus", "Bike", "Goods Van", "Agriculture"],
    default="Other"
)

# Reformat Year as datetime (Jan 1 of each year)
df['Year'] = pd.to_datetime(df['Year'].astype(str) + "-01-01")

# ------------------------------
# Upload to MySQL
# ------------------------------
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
)

df.to_sql("accidents", con=engine, if_exists="replace", index=False)

print("✅ Data successfully loaded into MySQL table 'accidents'")
