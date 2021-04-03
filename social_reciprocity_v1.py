### Info Viz
### Homework Three Companion Python File
# The purpose of this file is to calculate the social reciprocity and social amplitude for each contact
# for 2020, for visualizing in D3

# Read file of messages
# goal is to aggregate to one row per contact, one column per... month? Week? TBD... will see how it looks
import pandas as pd

# read in CSV
date_cols = ["timestamp"]
df = pd.read_csv("testing_output_v8.csv", parse_dates=date_cols)

# makes counting eaiser
df['counter'] = 1
len(df)

# get the date of first contact (this may be an interesting variable to organize a plot by
# This didn't work for some reason
#first_contact_old = df.groupby('other_person').agg({'timestamp':'first'})

# this looks like it worked. But it will be a bit trickier to merge it back in
first_contact = df.groupby('other_person').apply(lambda x:x[x.timestamp==min(x.timestamp)])




# first, reduce to 2020 data and beyond
df20 = df[df['timestamp'] > '2020-01-01']
len(df20)

# the first step will be simple - aggregate by word count and message frequency
#contact_totals = df20.groupby(["other_person", "sent_or_recieved"])["counter"].count()

contact_totals = df20.groupby(["other_person", "sent_or_recieved"]).agg({'counter': 'sum', 'word_count':'sum', 'polarity':['mean', 'median', 'var']})
contact_totals = contact_totals.reset_index()
contact_totals.columns = ['other_person', 'sent_or_recieved', 'counter','word_count', 'polarity_mean', 'polarity_median', 'polarity_var']


ct2 = contact_totals.pivot(index="other_person", columns="sent_or_recieved", values=["counter",'word_count', 'polarity_mean', 'polarity_median', 'polarity_var'])
ct2['msg_reciprocity'] = ct2['counter', 'sent'] / (ct2['counter', 'sent'] + ct2['counter', 'recieved'])

ct2['words_reciprocity'] = ct2['word_count', 'sent'] / (ct2['word_count', 'sent'] + ct2['word_count', 'recieved'])

# divided by two as this is the range of the polarity measures. But first add one to ensure everything is positive
ct2['polarity_mean', 'sent'] = ct2['polarity_mean', 'sent'] + 1
ct2['polarity_mean', 'recieved'] = ct2['polarity_mean', 'recieved'] + 1
ct2['avg_polarity_reciprocity'] = (ct2['polarity_mean', 'sent'] - ct2['polarity_mean', 'recieved']) / 2

# add .5 to ensure the line of social reciprocity is still at 0.5.
ct2['avg_polarity_reciprocity'] = ct2['avg_polarity_reciprocity'] + 0.5

# repeat for median

# divided by two as this is the range of the polarity measures. But first add one to ensure everything is positive
ct2['polarity_median', 'sent'] = ct2['polarity_median', 'sent'] + 1
ct2['polarity_median', 'recieved'] = ct2['polarity_median', 'recieved'] + 1
ct2['median_polarity_reciprocity'] = (ct2['polarity_mean', 'sent'] - ct2['polarity_median', 'recieved']) / 2

# add .5 to ensure the line of social reciprocity is still at 0.5.
ct2['median_polarity_reciprocity'] = ct2['median_polarity_reciprocity'] + 0.5


# two measures of social amplitude
ct2['message_amplitude'] = ct2['counter', 'sent'] + ct2['counter', 'recieved']
ct2['word_amplitude'] = ct2['word_count', 'sent'] + ct2['word_count', 'recieved']

# merge back in first contact
ct3 = ct2.merge(first_contact[['timestamp']], left_on='other_person', right_on='other_person')

# select just the columns we need:
y = ct3[ct3.columns[-7:]]

# flatten column names
y.columns = [''.join(col) if type(col) is tuple else col for col in y.columns.values]
len(y)

# drop anyone who hasn't both sent and received a message
y = y.dropna(thresh=4)
len(y)

# write out for d3
y.to_csv("social_reciprocity_v2.csv", header=True, index=True)




