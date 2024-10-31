import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
import seaborn as sns
import random
sns.set(style="whitegrid")

#1.1 Create dataframes merging information from multiple datasets
#load the hourly data files
hourly_steps = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/hourlySteps_merged.csv")
hourly_intensities = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/hourlyIntensities_merged.csv")
hourly_calories = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/hourlyCalories_merged.csv")

#load the minutely data files
minute_calories = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/minuteCaloriesNarrow_merged.csv")
minute_intensities = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/minuteIntensitiesNarrow_merged.csv")
minute_mets = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/minuteMETsNarrow_merged.csv")

weight = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/weightLogInfo_merged.csv")
#merge hourly data
hourly_data = hourly_steps.merge(hourly_intensities, on=["Id", "ActivityHour"]) \
                          .merge(hourly_calories, on=["Id", "ActivityHour"])

#merge minutely data
minutely_data = minute_calories.merge(minute_intensities, on=["Id", "ActivityMinute"]) \
                               .merge(minute_mets, on=["Id", "ActivityMinute"])


#1.2 Convert time strings into datatime type
daily_activity = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/dailyActivity_merged.csv")
sleep_day = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/sleepDay_merged.csv")

hourly_data['Date'] = pd.to_datetime(hourly_data['ActivityHour'], format='%m/%d/%Y %I:%M:%S %p')

minutely_data['Date'] = pd.to_datetime(minutely_data['ActivityMinute'], format='%m/%d/%Y %I:%M:%S %p')

daily_activity['Date'] = pd.to_datetime(daily_activity['ActivityDate'], format='%m/%d/%Y')

sleep_day['Date'] = pd.to_datetime(sleep_day['SleepDay'], format='%m/%d/%Y %I:%M:%S %p')


#1.3 Process heart rate data for every 5 seconds
heart_rate_data = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/heartrate_seconds_merged.csv")
heart_rate_data['Time'] = pd.to_datetime(heart_rate_data['Time'])
#extracting the minute data
heart_rate_data['Minute'] = heart_rate_data['Time'].dt.floor('min')
#average per minute and add as a new column
heart_rate_avg = heart_rate_data.groupby(['Id', 'Minute'])['Value'].mean().reset_index()
heart_rate_avg.columns = ['Id', 'Date', 'Avg_HeartRate']
#merge with minuteley dataframe
merged_by_minute = pd.merge(minutely_data, heart_rate_avg, on=['Id', 'Date'], how='left')
#clear NA lines
merged_by_minute = merged_by_minute[merged_by_minute['Avg_HeartRate'].notna()]

#2.1 Data visualization, distribution and outliers
#Part a
selected_day = pd.to_datetime('2016-04-15').date()
day_data = merged_by_minute[merged_by_minute['Date'].dt.date == selected_day]
#select the users
selected_users = day_data['Id'].unique()[:10]
filtered_data = day_data[day_data['Id'].isin(selected_users)]
#plot each users heartrate
plt.figure(figsize=(15, 6))
for user_id in selected_users:
    user_data = filtered_data[filtered_data['Id'] == user_id]
    plt.plot(user_data['Date'], user_data['Avg_HeartRate'], label=f'User {user_id}')
#plot details
plt.title('Average minute Heartrate for 10 random users', fontsize=16)
plt.xlabel('Time of Day', fontsize=14)
plt.ylabel('Average Heart Rate', fontsize=14)
plt.xticks(rotation=45)
plt.legend(title='User Id', loc='best')
plt.grid(True)
plt.tight_layout()

#partb sleep for a month
sleep_day['Date'] = pd.to_datetime(sleep_day['SleepDay']).dt.date
# Select the first 5 unique users to analyze
selected_users = sleep_day['Id'].unique()[:5]
# Filter the data for the selected users only
filtered_sleep_data = sleep_day[sleep_day['Id'].isin(selected_users)]
# Aggregate by user and date to get the daily sleep duration
daily_sleep_data = filtered_sleep_data.groupby(['Id', 'Date']).agg({'TotalMinutesAsleep': 'mean'}).reset_index()
# Plotting daily sleep duration for each selected user
plt.figure(figsize=(15, 6))
for user_id, user_data in daily_sleep_data.groupby('Id'):
    plt.plot(user_data['Date'], user_data['TotalMinutesAsleep'], marker='o', label=f'User {user_id}')
#plot
plt.title('Daily Sleep Duration for 5 random users')
plt.xlabel('Date selected')
plt.ylabel('Total Minutes Asleep')
plt.xticks(rotation=45)
plt.legend(title='User Id')
plt.grid(True)
plt.tight_layout()

#partc steps for a month
hourly_steps_data = pd.read_csv("/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/hourlySteps_merged.csv")
hourly_steps_data['ActivityHour'] = pd.to_datetime(hourly_steps_data['ActivityHour'])
hourly_steps_data['OnlyDate'] = hourly_steps_data['ActivityHour'].dt.date
#filerting for specific time range
date_range_start = pd.to_datetime('2016-04-12').date()
date_range_end = pd.to_datetime('2016-05-12').date()
filtered_data = hourly_steps_data[(hourly_steps_data['OnlyDate'] >= date_range_start) & (hourly_steps_data['OnlyDate'] <= date_range_end)]
#summing hourly steps per day for each user
daily_steps_summary = filtered_data.groupby(['Id', 'OnlyDate'])['StepTotal'].sum().reset_index()
#select a subset of 5 users
user_subset = random.sample(list(daily_steps_summary['Id'].unique()), 5)
#initialize the plot
plt.figure(figsize=(15, 7))
#plot the daily steps
for user in user_subset:
    user_steps = daily_steps_summary[daily_steps_summary['Id'] == user]
    plt.plot(user_steps['OnlyDate'], user_steps['StepTotal'], marker='o', label=f'User {user}')
#plot stats
plt.title('Daily Steps for Selected Users (April-May 2016)', fontsize=16)
plt.xlabel('Date', fontsize=14)
plt.ylabel('Total Steps Per Day', fontsize=14)
plt.xticks(rotation=45)
plt.legend(title='User ID')
plt.grid(True)

#partd weight
#datetime
weight['Date'] = pd.to_datetime(weight['Date'])
#identify user with most weigth
top_user_id = weight['Id'].mode()[0]
#extract data
top_user_weight_data = weight[weight['Id'] == top_user_id]
#set up plot
plt.figure(figsize=(12, 6))
#plot it
plt.plot(top_user_weight_data['Date'], top_user_weight_data['WeightKg'], marker='o')
plt.title(f'Weight Change Over Time for User: {top_user_id}')
plt.xlabel('Date')
plt.ylabel('Weight in KG')
plt.xticks(rotation=45)
plt.grid(visible=True, linestyle='--')

#2.2 Relationships with variables
#a create 2 scatterplots and compare
#Scatter plot for StepTotal vs. Calories
plt.figure(figsize=(10, 6))
plt.scatter(hourly_data['StepTotal'], hourly_data['Calories'], alpha=0.5, c='blue')
plt.title('Relationship Between StepTotal and Calories')
plt.xlabel('StepTotal')
plt.ylabel('Calories')
plt.grid(True)

#Scatterplot for Total intensity vs calories
plt.figure(figsize=(10, 6))
plt.scatter(hourly_data['TotalIntensity'], hourly_data['Calories'], alpha=0.5, c='g')
plt.title('Relationship Between TotalIntensity and Calories')
plt.xlabel('TotalIntensity')
plt.ylabel('Calories')
plt.grid(True)

#b) Scatter plot for TotalMinutesAsleep vs. TotalTimeInBed
plt.figure(figsize=(10, 6))
plt.scatter(sleep_day['TotalMinutesAsleep'], sleep_day['TotalTimeInBed'], alpha=0.5, c='grey')
plt.title('Correlation Between TotalMinutesAsleep and TotalTimeInBed')
plt.xlabel('Total Minutes Asleep')
plt.ylabel('Total Time In Bed')
plt.grid(True)

#c Merging dailyActivity and sleepDay datasets on 'Id' and 'Date'
# Convert 'Date' columns to the same format in both dataframes before merging
daily_activity['Date'] = pd.to_datetime(daily_activity['Date']).dt.date
sleep_day['Date'] = pd.to_datetime(sleep_day['Date']).dt.date

# Now, try merging again
merged_data = pd.merge(daily_activity, sleep_day, on=['Id', 'Date'], how='inner')
#plot
plt.figure(figsize=(10, 6))
plt.scatter(merged_data['TotalMinutesAsleep'], merged_data['SedentaryMinutes'], alpha=0.5, c='orange')
plt.title('Relationship Between TotalMinutesAsleep and SedentaryMinutes')
plt.xlabel('Total Minutes Asleep')
plt.ylabel('Sedentary Minutes')
plt.grid(True)

#2.3 data visualization trends over time
selected_users = hourly_data['Id'].unique()[:3]
filtered_data = hourly_data[hourly_data['Id'].isin(selected_users)]
#convert 'ActivityHour' to datetime if needed
filtered_data['ActivityHour'] = pd.to_datetime(filtered_data['ActivityHour'])
filtered_data['Hour'] = filtered_data['ActivityHour'].dt.hour
#calculate the average intensity for each hour per user
hourly_intensity = filtered_data.groupby(['Id', 'Hour'])['AverageIntensity'].mean().unstack()
#plotting
plt.figure(figsize=(14, 7))
hourly_intensity.T.plot(kind='bar', width=0.8, figsize=(15, 7))
plt.title('Distribution of Average Intensity Over a Day for Selected Users', fontsize=16)
plt.xlabel('Hour of the Day', fontsize=14)
plt.ylabel('Average Intensity', fontsize=14)
plt.xticks(rotation=0)
plt.legend(title='User Id')
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)

#3:T-TEST
# Sample dataframes, replace with your actual file paths
daily_steps = pd.read_csv('/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/dailySteps_merged.csv')
sleep_data = pd.read_csv('/Users/shriatluri/Desktop/archive/mturkfitbit_export_4.12.16-5.12.16/Fitabase Data 4.12.16-5.12.16/sleepDay_merged.csv')

daily_steps['Date'] = pd.to_datetime(daily_steps['ActivityDay'], errors='coerce')
sleep_data['Date'] = pd.to_datetime(sleep_data['SleepDay'], errors='coerce').dt.date

# Convert sleep_data 'Date' to datetime type to match daily_steps 'Date'
sleep_data['Date'] = pd.to_datetime(sleep_data['Date'])

# Proceed with the merge
merged_data = pd.merge(
    daily_steps[['Id', 'Date', 'StepTotal']],
    sleep_data[['Id', 'Date', 'TotalTimeInBed']],
    on=['Id', 'Date'],
    how='inner'
)

median_steps = merged_data['StepTotal'].median()
high_steps = merged_data[merged_data['StepTotal'] > median_steps]
low_steps = merged_data[merged_data['StepTotal'] <= median_steps]

# Perform the t-test
t_stat1, p_val1 = stats.ttest_ind(high_steps['StepTotal'], low_steps['StepTotal'], equal_var=False)

# Print results for TotalSteps
print("T-Test for TotalSteps Groups:")
print(f"T-statistic: {t_stat1:.4f}, P-value: {p_val1:.4g}")

if p_val1 < 0.10:
    print("Reject the null hypothesis: Significant difference in Calories between high and low TotalSteps groups.\n")
else:
    print("Fail to reject the null hypothesis: No significant difference in Calories between high and low TotalSteps groups.\n")

# Descriptive statistics
print("High Steps group (TotalSteps):")
print(high_steps['StepTotal'].describe())

print("\nLow Steps group (TotalSteps):")
print(low_steps['StepTotal'].describe())

# T-Test 2: Compare high vs low TotalTimeInBed groups on Calories
# Set threshold at the median of TotalTimeInBed
median_time_in_bed = merged_data['TotalTimeInBed'].median()
high_time_in_bed = merged_data[merged_data['TotalTimeInBed'] > median_time_in_bed]
low_time_in_bed = merged_data[merged_data['TotalTimeInBed'] <= median_time_in_bed]

# Perform the t-test
t_stat2, p_val2 = stats.ttest_ind(high_time_in_bed['TotalTimeInBed'], low_time_in_bed['TotalTimeInBed'], equal_var=False)

# Print results for TotalTimeInBed
print("\nT-Test for TotalTimeInBed Groups:")
print(f"T-statistic: {t_stat2:.4f}, P-value: {p_val2:.4g}")

if p_val2 < 0.10:
    print("Reject the null hypothesis: Significant difference in Calories between high and low TotalTimeInBed groups.\n")
else:
    print("Fail to reject the null hypothesis: No significant difference in Calories between high and low TotalTimeInBed groups.\n")

# Descriptive statistics
print("High Time In Bed group (TotalTimeInBed):")
print(high_time_in_bed['TotalTimeInBed'].describe())

print("\nLow Time In Bed group (TotalTimeInBed):")
print(low_time_in_bed['TotalTimeInBed'].describe())