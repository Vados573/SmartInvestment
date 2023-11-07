import pandas as pd
import numpy as np
import sys

# Read the path to folder containing csv files
path_to_csv = sys.argv[1]
# path_to_csv = 'market_data'

# Create an empty dataframe
combined_df = pd.DataFrame()

# Define the composition of our star fund
star_fund_comp = {
    "META": 0.15,
    "NFLX": 0.10,
    "AAPL": 0.25,
    "TSLA": 0.15,
    "GOOGL": 0.20,
    "AMZN": 0.15,
}
# Loop through the dictionary that contains our star fund composition
for symbol, weight in star_fund_comp.items():
    # Create the dataframe that will hold the data of the two csv files of each stock
    stock_data = pd.DataFrame()
    # Since each stock has 2 csv files we loop twice in reverse order to get dates sorted
    for i in range(2, 0, -1):
        # Open the csv file
        file_name = f"{symbol}_{i}.csv"
        temp_data = pd.read_csv(path_to_csv + '/' + file_name, usecols=[0, 4])
        # Add a column called Stock that will contain the name of the stock
        temp_data["Stock"] = symbol
        # Add a column called weight that will contain the weight of that stock in our star fund
        temp_data["Weight"] = weight
        # Concat the first and second csv file for each stock
        stock_data = pd.concat([stock_data, temp_data], ignore_index=True)
    # Create a new dataframe that will contain the data of all the stock combined
    combined_df = pd.concat([combined_df, stock_data], ignore_index=True)

# We parse our Date column
combined_df['Date'] = combined_df['Date'].str.replace(r'[-+]\d{2}:\d{2}$', '', regex=True)
combined_df['Date'] = pd.to_datetime(combined_df['Date'], format='%Y-%m-%d %H:%M:%S')
# We create a column called last_close that will contain the close value of the previous row (Day)
combined_df['last_close'] = combined_df['Close'].shift(1)
# The last line doesn't take into consideration when we change stock,
# so we need to remove last_close for the first day of each stock
combined_df['last_close'] = np.where(combined_df['Stock'] == combined_df['Stock'].shift(1),
                                     combined_df['last_close'], np.nan)
# We calculate our fund's daily Gain/Loss % using the formula:
# Daily Gain/Loss = The difference of the close price of a day minus the close price of the previous day
# We divide by the close price of the previous day and multiply by 100 then we apply the weight
combined_df['Fund Gain/Loss %'] = (combined_df['Close'] - combined_df['last_close']) \
                                  / combined_df['last_close'] * 100 * combined_df['Weight']

# We sum the Gain/Loss % while grouping by Date which will give the daily Gain/Loss % of our fund
combined_df = combined_df.groupby(['Date'], as_index=False)['Fund Gain/Loss %'].sum()

# We create a column that will hold the daily accumulated Gain/Loss % of our fund, and we initialize it with 0
combined_df['Accumulated Gain/Loss %'] = 0.0
accumulated_percentage = 0.0
# We loop into each row of our Fund's dataframe, and we calculate the accumulated percentage
for index, row in combined_df.iterrows():
    accumulated_percentage += row['Fund Gain/Loss %']
    combined_df.at[index, 'Accumulated Gain/Loss %'] = accumulated_percentage

# We open users.csv that contains the data of our investors
users_df = pd.read_csv(path_to_csv + '/users.csv')

# We add two columns where we will store the Accumulate Gain/Loss of both the open and close date of each investor
users_df['Fund Cumulative Open'] = users_df['investment_open_date'].map(
    combined_df.set_index('Date')['Accumulated Gain/Loss %'])

users_df['Fund Cumulative Close'] = users_df['investment_close_date'].map(
    combined_df.set_index('Date')['Accumulated Gain/Loss %'])

# We use the following formula :
# amount_invested * (1 + "cumulated performance on close"/100 - "cumulated performance on open"/100)
users_df['amount_refund'] = users_df['amount_invested'] * (
        1 + users_df['Fund Cumulative Close'] / 100 - users_df['Fund Cumulative Open'] / 100)

# Let's drop the columns that we are not using anymore
users_df = users_df.drop(columns=['Fund Cumulative Close', 'Fund Cumulative Open'])

# We print the result into the csv file
users_df.to_csv(path_to_csv + '/users_refund.csv', index=False, decimal='.', date_format='%Y-%m-%d')
