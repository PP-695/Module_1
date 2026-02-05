#!/usr/bin/env python
# coding: utf-8
"""
Homework: Green Taxi Data Analysis - November 2025
"""

import pandas as pd
import os

# =============================================================================
# Download Data (if not already downloaded)
# =============================================================================

# Download green taxi data for November 2025
green_taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
green_taxi_file = "green_tripdata_2025-11.parquet"

zones_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
zones_file = "taxi_zone_lookup.csv"

# Download files if they don't exist
if not os.path.exists(green_taxi_file):
    print(f"Downloading {green_taxi_file}...")
    os.system(f"wget {green_taxi_url}")
else:
    print(f"{green_taxi_file} already exists")

if not os.path.exists(zones_file):
    print(f"Downloading {zones_file}...")
    os.system(f"wget {zones_url}")
else:
    print(f"{zones_file} already exists")

# =============================================================================
# Load Data
# =============================================================================

print("\nLoading data...")
df = pd.read_parquet(green_taxi_file)
zones = pd.read_csv(zones_file)

print(f"Green taxi data shape: {df.shape}")
print(f"Zones data shape: {zones.shape}")

print("\nGreen taxi columns:")
print(df.columns.tolist())

print("\nZones columns:")
print(zones.columns.tolist())

print("\nFirst few rows of green taxi data:")
print(df.head())

print("\nData types:")
print(df.dtypes)

# =============================================================================
# Question 3: Counting short trips
# For trips in November 2025 (lpep_pickup_datetime between '2025-11-01' 
# and '2025-12-01', exclusive of upper bound), how many trips had 
# trip_distance <= 1 mile?
# =============================================================================

print("\n" + "="*60)
print("QUESTION 3: Counting short trips")
print("="*60)

# Filter for November 2025
nov_trips = df[
    (df['lpep_pickup_datetime'] >= '2025-11-01') & 
    (df['lpep_pickup_datetime'] < '2025-12-01')
]
print(f"Total trips in November 2025: {len(nov_trips)}")

# Count trips with distance <= 1 mile
short_trips = nov_trips[nov_trips['trip_distance'] <= 1]
print(f"Trips with distance <= 1 mile: {len(short_trips)}")

# =============================================================================
# Question 4: Longest trip for each day
# Which pickup day had the longest trip distance?
# Only consider trips with trip_distance < 100 miles
# =============================================================================

print("\n" + "="*60)
print("QUESTION 4: Longest trip for each day")
print("="*60)

# Filter for valid trips (distance < 100 miles)
valid_trips = df[df['trip_distance'] < 100].copy()

# Find the trip with the longest distance
longest_trip = valid_trips.loc[valid_trips['trip_distance'].idxmax()]
print(f"Longest trip distance: {longest_trip['trip_distance']} miles")
print(f"Pickup datetime: {longest_trip['lpep_pickup_datetime']}")
print(f"Pickup day: {longest_trip['lpep_pickup_datetime'].date()}")

# Alternative: Group by date and find max distance per day
valid_trips['pickup_date'] = valid_trips['lpep_pickup_datetime'].dt.date
max_distance_by_day = valid_trips.groupby('pickup_date')['trip_distance'].max()
longest_day = max_distance_by_day.idxmax()
print(f"\nDay with longest trip: {longest_day}")
print(f"Distance on that day: {max_distance_by_day[longest_day]} miles")

# =============================================================================
# Question 5: Biggest pickup zone
# Which pickup zone had the largest total_amount (sum) on November 18th, 2025?
# =============================================================================

print("\n" + "="*60)
print("QUESTION 5: Biggest pickup zone")
print("="*60)

# Filter for November 18th, 2025
nov_18_trips = df[
    (df['lpep_pickup_datetime'] >= '2025-11-18') & 
    (df['lpep_pickup_datetime'] < '2025-11-19')
]
print(f"Trips on November 18th, 2025: {len(nov_18_trips)}")

# Group by pickup location and sum total_amount
pickup_totals = nov_18_trips.groupby('PULocationID')['total_amount'].sum()

# Get the location with highest total
top_pickup_id = pickup_totals.idxmax()
top_pickup_amount = pickup_totals[top_pickup_id]

# Get the zone name
top_zone = zones[zones['LocationID'] == top_pickup_id]['Zone'].values[0]
print(f"Pickup zone with largest total_amount: {top_zone}")
print(f"Location ID: {top_pickup_id}")
print(f"Total amount: ${top_pickup_amount:.2f}")

# Show top 5 zones
print("\nTop 5 pickup zones by total_amount on Nov 18:")
top_5 = pickup_totals.sort_values(ascending=False).head(5)
for loc_id, amount in top_5.items():
    zone_name = zones[zones['LocationID'] == loc_id]['Zone'].values[0]
    print(f"  {zone_name}: ${amount:.2f}")

# =============================================================================
# Question 6: Largest tip
# For passengers picked up in "East Harlem North" in November 2025,
# which drop-off zone had the largest tip?
# =============================================================================

print("\n" + "="*60)
print("QUESTION 6: Largest tip")
print("="*60)

# Get the LocationID for "East Harlem North"
east_harlem_north_id = zones[zones['Zone'] == 'East Harlem North']['LocationID'].values[0]
print(f"East Harlem North LocationID: {east_harlem_north_id}")

# Filter for November 2025 trips picked up in East Harlem North
nov_trips_ehn = df[
    (df['lpep_pickup_datetime'] >= '2025-11-01') & 
    (df['lpep_pickup_datetime'] < '2025-12-01') &
    (df['PULocationID'] == east_harlem_north_id)
]
print(f"Trips from East Harlem North in November 2025: {len(nov_trips_ehn)}")

# Find the trip with the largest tip
largest_tip_trip = nov_trips_ehn.loc[nov_trips_ehn['tip_amount'].idxmax()]
dropoff_id = largest_tip_trip['DOLocationID']
largest_tip = largest_tip_trip['tip_amount']

# Get the drop-off zone name
dropoff_zone = zones[zones['LocationID'] == dropoff_id]['Zone'].values[0]
print(f"Drop-off zone with largest tip: {dropoff_zone}")
print(f"Drop-off LocationID: {int(dropoff_id)}")
print(f"Tip amount: ${largest_tip:.2f}")

# Show top drop-off zones by max tip
print("\nTop 5 drop-off zones by max tip (from East Harlem North pickups):")
top_tips = nov_trips_ehn.groupby('DOLocationID')['tip_amount'].max().sort_values(ascending=False).head(5)
for loc_id, tip in top_tips.items():
    zone_name = zones[zones['LocationID'] == loc_id]['Zone'].values[0]
    print(f"  {zone_name}: ${tip:.2f}")

# =============================================================================
# Summary of Answers
# =============================================================================

print("\n" + "="*60)
print("SUMMARY OF ANSWERS")
print("="*60)
print(f"Question 3: {len(short_trips)} short trips (<= 1 mile)")
print(f"Question 4: {longest_day} (longest trip day)")
print(f"Question 5: {top_zone} (highest total_amount on Nov 18)")
print(f"Question 6: {dropoff_zone} (largest tip from East Harlem North)")
