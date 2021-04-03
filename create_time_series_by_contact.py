### Info Viz
### Homework Two Companion Python File
# The purpose of this file is to aggregate

# Read file of messages
# goal is to aggregate to one row per contact, one column per... month? Week? TBD... will see how it looks
import pandas as pd

# read in CSV
date_cols = ["timestamp"]
df = pd.read_csv("testing_output_v8.csv", parse_dates=date_cols)

# makes counting eaiser
df['counter'] = 1


# aggregate count per week, per person
weekly_df = df.groupby([pd.Grouper(key='timestamp', freq='W'), 'other_person']).counter.sum()
weekly_df = pd.DataFrame(weekly_df)
weekly_df = weekly_df.reset_index()

# each column should be the start of the time range
weekly_df = weekly_df.pivot(index="other_person", columns="timestamp")
weekly_df = weekly_df.fillna(0)  # replace nan with zero

# get rid of the multi index for easy export
weekly_df.columns = weekly_df.columns.droplevel()
weekly_df.to_csv("weekly_time_series.csv", header=True, index=True)


## Weekly data was a bit choppy, try monthly aggregation
# aggregate count per week, per person
monthly_df = df.groupby([pd.Grouper(key='timestamp', freq='M'), 'other_person']).counter.sum()
monthly_df = pd.DataFrame(monthly_df)
monthly_df = monthly_df.reset_index()

# each column should be the start of the time range
monthly_df = monthly_df.pivot(index="other_person", columns="timestamp")
monthly_df = monthly_df.fillna(0)  # replace nan with zero

# get rid of the multi index for easy export
monthly_df.columns = monthly_df.columns.droplevel()
monthly_df.to_csv("monthly_time_series.csv", header=True, index=True)

## in the future could try smoothing the weeks with a running average






