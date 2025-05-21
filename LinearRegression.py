import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# load data from excel
# data in excel has following columns and sample data
#   CUSTOMER NAME    CommitStatus    PartnerName     PRODUCT Group     After_EA_enrollment_ALC    After_EA_enrollment_EA         
#   Customer Name323 Full            NETWORK         Center of excell  110                        10
#   Customer Name323 Full            NETWORK         WOKE Suite        100                        20
#   Customer Name323 partial         NETWORK         NXT               50                         40
#   Customer Name323 Partial         NETWORK         Access            40                         45
#   Customer Name123 Partial         NETWORK         Core              30                         23
#   Customer Name123 Full            NETWORK         Mid Routing       20                         12
#   Customer Name123 Full            NETWORK         My Wireless       30                         56
#   Customer Name123 Partial         NETWORK         Firewall          40                         12
# find linear regression between Customer Name, Partner Name CommitStatus, Product Group Combination and Amounts on date columns

def perform_regression_on_group(df, group_keys):
    # Check if there are enough data points for regression
    if len(df) < 2:
        print(f"Not enough data for group: {group_keys}")
        return None

    # Use 'After_EA_enrollment_ALC' as the feature (independent variable)
    # and 'After_EA_enrollment_EA' as the target (dependent variable)
    X = df[['UNKNOWN_TotalAfterMysku']].values
    #y = df['GrandTotal'].values

    # Initialize and fit the linear regression model
    lr_model = LinearRegression()
    lr_model.fit(X, y)

    # Retrieve the coefficient and intercept of the model
    coef = lr_model.coef_[0]
    intercept = lr_model.intercept_

    print(f"Regression for group {group_keys}: Coefficient = {coef}, Intercept = {intercept}")

    return lr_model

def main():
    # Load data from excel
    # Assuming the excel file is named 'data.xlsx' and is located in the same folder as this script.
    try:
        data = pd.read_excel('EA-Customer-Purchases -Testfile2.xlsx')
    except Exception as e:
        print("Error loading excel file:", e)
        return

    # Define the grouping columns as per the task description:
    # Customer Name, CommitStatus, PartnerName, PRODUCT Group
    group_columns = ['CAVEndCustomerBUName', 'CommitStatus', 'PartnerName', 'InternalSubBusinessEntityName']

    # Group the data based on the specified combination
    grouped = data.groupby(group_columns)
    regression_models = {}

    # Iterate over each group and perform linear regression amounts
    for group_keys, group_df in grouped:
        model = perform_regression_on_group(group_df, group_keys)
        regression_models[group_keys] = model

if __name__ == '__main__':
    main()
