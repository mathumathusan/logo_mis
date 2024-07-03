import streamlit as st
import pandas as pd
import sqlalchemy

# Load secrets from Streamlit secrets
secrets = st.secrets["connections"]["my_database"]

# Construct connection string
conn_string = f"mysql+pymysql://{secrets['username']}:{secrets['password']}@{secrets['host']}:{secrets['port']}/{secrets['database']}"

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(conn_string)

# Query the database to retrieve revenue table values into a DataFrame
revenue_query='''
SELECT 
    local_authorities.name AS name,
    provinces.name AS province_name,
    districts.name AS district_name,
    actual_budgets.month,
    actual_budgets.year,
    MAX(CASE WHEN revenues.id = 1 THEN annual_budget_details.total_amount ELSE 0 END) AS RateTaxesBudget,
    MAX(CASE WHEN revenues.id = 1 THEN actual_budget_details.total_amount ELSE 0 END) AS RateTaxesActual,
    MAX(CASE WHEN revenues.id = 2 THEN annual_budget_details.total_amount ELSE 0 END) AS RentBudget,
    MAX(CASE WHEN revenues.id = 2 THEN actual_budget_details.total_amount ELSE 0 END) AS RentActual,
    MAX(CASE WHEN revenues.id = 3 THEN annual_budget_details.total_amount ELSE 0 END) AS LicenseBudget,
    MAX(CASE WHEN revenues.id = 3 THEN actual_budget_details.total_amount ELSE 0 END) AS LicenseActual,
    MAX(CASE WHEN revenues.id = 4 THEN annual_budget_details.total_amount ELSE 0 END) AS FeesForServiceBudget,
    MAX(CASE WHEN revenues.id = 4 THEN actual_budget_details.total_amount ELSE 0 END) AS FeesForServiceActual,
    MAX(CASE WHEN revenues.id = 5 THEN annual_budget_details.total_amount ELSE 0 END) AS WarrantCostFinePenaltiesBudget,
    MAX(CASE WHEN revenues.id = 5 THEN actual_budget_details.total_amount ELSE 0 END) AS WarrantCostFinePenaltiesActual,
    MAX(CASE WHEN revenues.id = 6 THEN annual_budget_details.total_amount ELSE 0 END) AS OtherRevenueBudget,
    MAX(CASE WHEN revenues.id = 6 THEN actual_budget_details.total_amount ELSE 0 END) AS OtherRevenueActual,
    MAX(CASE WHEN revenues.id = 7 THEN annual_budget_details.total_amount ELSE 0 END) AS RevenueGrantsAllBudget,
    MAX(CASE WHEN revenues.id = 7 THEN actual_budget_details.total_amount ELSE 0 END) AS RevenueGrantsAllActual,
    MAX(CASE WHEN revenues.id = 8 THEN annual_budget_details.total_amount ELSE 0 END) AS RevenueGrantsOtherBudget,
    MAX(CASE WHEN revenues.id = 8 THEN actual_budget_details.total_amount ELSE 0 END) AS RevenueGrantsOtherActual,
    MAX(CASE WHEN revenues.id = 9 THEN annual_budget_details.total_amount ELSE 0 END) AS CapitalGrantsBudget,
    MAX(CASE WHEN revenues.id = 9 THEN actual_budget_details.total_amount ELSE 0 END) AS CapitalGrantsActual,
    MAX(CASE WHEN revenues.id = 10 THEN annual_budget_details.total_amount ELSE 0 END) AS CapitalLoansBudget,
    MAX(CASE WHEN revenues.id = 10 THEN actual_budget_details.total_amount ELSE 0 END) AS CapitalLoansActual,
    MAX(CASE WHEN revenues.id = 15 THEN annual_budget_details.total_amount ELSE 0 END) AS SaleOfCapitalAssetsBudget,
    MAX(CASE WHEN revenues.id = 15 THEN actual_budget_details.total_amount ELSE 0 END) AS SaleOfCapitalAssetsActual,
    MAX(CASE WHEN revenues.id = 16 THEN annual_budget_details.total_amount ELSE 0 END) AS OtherCapitalReceiptsBudget,
    MAX(CASE WHEN revenues.id = 16 THEN actual_budget_details.total_amount ELSE 0 END) AS OtherCapitalReceiptsActual
FROM 
    actual_budgets
INNER JOIN 
    actual_budget_details ON actual_budgets.id = actual_budget_details.actual_budget_id
INNER JOIN 
    local_authorities ON local_authorities.id = actual_budgets.local_authority_id
INNER JOIN 
    districts ON districts.id = local_authorities.district_id
INNER JOIN 
    provinces ON provinces.id = districts.province_id 
INNER JOIN 
    revenues ON revenues.id = actual_budget_details.revenue_id
LEFT JOIN 
    annual_budgets ON annual_budgets.local_authority_id = local_authorities.id AND annual_budgets.year = actual_budgets.year
LEFT JOIN 
    annual_budget_details ON annual_budget_details.annual_budget_id = annual_budgets.id AND annual_budget_details.revenue_id = revenues.id
GROUP BY 
    local_authorities.name,
    provinces.name, 
    districts.name, 
    actual_budgets.month, 
    actual_budgets.year
ORDER BY
    local_authorities.name,
    actual_budgets.year,
    actual_budgets.month;
''';

revenue_df = pd.read_sql_query(revenue_query, engine)

revenue_df.fillna(0, inplace=True)



# Calculate the total recurrent revenue budget and actual amounts
revenue_df['RecurrentRevenueTotalBudget'] = (
    revenue_df['RateTaxesBudget'] + revenue_df['RentBudget'] + revenue_df['LicenseBudget'] +
    revenue_df['FeesForServiceBudget'] + revenue_df['WarrantCostFinePenaltiesBudget'] +
    revenue_df['OtherRevenueBudget'] + revenue_df['RevenueGrantsAllBudget'] + revenue_df['RevenueGrantsOtherBudget']
)
revenue_df['RecurrentRevenueTotalActual'] = (
    revenue_df['RateTaxesActual'] + revenue_df['RentActual'] + revenue_df['LicenseActual'] +
    revenue_df['FeesForServiceActual'] + revenue_df['WarrantCostFinePenaltiesActual'] +
    revenue_df['OtherRevenueActual'] + revenue_df['RevenueGrantsAllActual'] + revenue_df['RevenueGrantsOtherActual']
)

revenue_df['NonRecurrentRevenueTotalBudget'] = (
    revenue_df['CapitalGrantsBudget'] + revenue_df['CapitalLoansBudget'] + revenue_df['SaleOfCapitalAssetsBudget'] +
    revenue_df['OtherCapitalReceiptsBudget']
)
revenue_df['NonRecurrentRevenueTotalActual'] = (
     revenue_df['CapitalGrantsActual'] + revenue_df['CapitalLoansActual'] +
    revenue_df['SaleOfCapitalAssetsActual'] + revenue_df['OtherCapitalReceiptsActual']
)

revenue_df = revenue_df[['name','month', 'year','province_name', 'district_name',
       'RateTaxesBudget','RateTaxesActual','RentBudget','RentActual',
       'LicenseBudget','LicenseActual','FeesForServiceBudget',
       'FeesForServiceActual','WarrantCostFinePenaltiesBudget','WarrantCostFinePenaltiesActual',
       'OtherRevenueBudget','OtherRevenueActual','RevenueGrantsAllBudget','RevenueGrantsAllActual',
       'RevenueGrantsOtherBudget','RevenueGrantsOtherActual','RecurrentRevenueTotalBudget',
       'RecurrentRevenueTotalActual','CapitalGrantsBudget','CapitalGrantsActual','CapitalLoansBudget',
       'CapitalLoansActual','SaleOfCapitalAssetsBudget','SaleOfCapitalAssetsActual','OtherCapitalReceiptsBudget',
       'OtherCapitalReceiptsActual','NonRecurrentRevenueTotalBudget','NonRecurrentRevenueTotalActual'
       ]]

# Query the database to retrieve expenditure table values into a DataFrame
expenditure_query='''
SELECT 
    local_authorities.name AS name,
    provinces.name AS province_name,
    districts.name AS district_name,
    actual_budgets.month,
    actual_budgets.year,
    MAX(CASE WHEN expenditures.id = 1 THEN annual_budget_details.total_amount ELSE 0 END) AS PersonalEmolumentBudget,
    MAX(CASE WHEN expenditures.id = 1 THEN actual_budget_details.total_amount ELSE 0 END) AS PersonalEmolumentsActual_this_month,
    MAX(CASE WHEN expenditures.id = 2 THEN annual_budget_details.total_amount ELSE 0 END) AS TravelingExpensesBudget,
    MAX(CASE WHEN expenditures.id = 2 THEN actual_budget_details.total_amount ELSE 0 END) AS TravelingExpensesActual_this_month,
    MAX(CASE WHEN expenditures.id = 3 THEN annual_budget_details.total_amount ELSE 0 END) AS SuppliesRequisitesBudget,
    MAX(CASE WHEN expenditures.id = 3 THEN actual_budget_details.total_amount ELSE 0 END) AS SuppliesRequisitesActual_this_month,
    MAX(CASE WHEN expenditures.id = 4 THEN annual_budget_details.total_amount ELSE 0 END) AS RepairsMaintenanceofCapitalAssetsBudget,
    MAX(CASE WHEN expenditures.id = 4 THEN actual_budget_details.total_amount ELSE 0 END) AS RepairsMaintenanceofCapitalAssetsActual_this_month,
    MAX(CASE WHEN expenditures.id = 5 THEN annual_budget_details.total_amount ELSE 0 END) AS TransportationCommunicationUtilityServiceBudget,
    MAX(CASE WHEN expenditures.id = 5 THEN actual_budget_details.total_amount ELSE 0 END) AS TransportationCommunicationUtilityServiceActual_this_month,
    MAX(CASE WHEN expenditures.id = 6 THEN annual_budget_details.total_amount ELSE 0 END) AS InterestPaymentsDividendsBudget,
    MAX(CASE WHEN expenditures.id = 6 THEN actual_budget_details.total_amount ELSE 0 END) AS InterestPaymentsDividendsActual_this_month,
    MAX(CASE WHEN expenditures.id = 7 THEN annual_budget_details.total_amount ELSE 0 END) AS GrantsContributionsSubsidiesBudget,
    MAX(CASE WHEN expenditures.id = 7 THEN actual_budget_details.total_amount ELSE 0 END) AS GrantsContributionsSubsidiesActual_this_month,
    MAX(CASE WHEN expenditures.id = 8 THEN annual_budget_details.total_amount ELSE 0 END) AS PensionsRetirementBenefitsGratuitiesBudget,
    MAX(CASE WHEN expenditures.id = 8 THEN actual_budget_details.total_amount ELSE 0 END) AS PensionsRetirementBenefitsGratuitiesActual_this_month,
    MAX(CASE WHEN expenditures.id = 9 THEN annual_budget_details.total_amount ELSE 0 END) AS CapitalExpenditureBudget,
    MAX(CASE WHEN expenditures.id = 9 THEN actual_budget_details.total_amount ELSE 0 END) AS CapitalExpenditureActual_this_month,
    MAX(CASE WHEN expenditures.id = 10 THEN annual_budget_details.total_amount ELSE 0 END) AS RehabilitationFundBudget,
    MAX(CASE WHEN expenditures.id = 10 THEN actual_budget_details.total_amount ELSE 0 END) AS RehabilitationFundActual_this_month,
    MAX(CASE WHEN expenditures.id = 11 THEN annual_budget_details.total_amount ELSE 0 END) AS LoanRepaymentsBudget,
    MAX(CASE WHEN expenditures.id = 11 THEN actual_budget_details.total_amount ELSE 0 END) AS LoanRepaymentActual_this_month,
    MAX(CASE WHEN expenditures.id = 18 THEN annual_budget_details.total_amount ELSE 0 END) AS AnyothercapitalexpenditureBudget,
    MAX(CASE WHEN expenditures.id = 18 THEN actual_budget_details.total_amount ELSE 0 END) AS AnyothercapitalexpenditureActual_this_month
FROM 
    actual_budgets
INNER JOIN 
    actual_budget_details ON actual_budgets.id = actual_budget_details.actual_budget_id
INNER JOIN 
    local_authorities ON local_authorities.id = actual_budgets.local_authority_id
INNER JOIN 
    districts ON districts.id = local_authorities.district_id
INNER JOIN 
    provinces ON provinces.id = districts.province_id 
INNER JOIN 
    expenditures ON expenditures.id = actual_budget_details.expenditure_id
LEFT JOIN 
    annual_budgets ON annual_budgets.local_authority_id = local_authorities.id AND annual_budgets.year = actual_budgets.year
LEFT JOIN 
    annual_budget_details ON annual_budget_details.annual_budget_id = annual_budgets.id AND annual_budget_details.expenditure_id = expenditures.id
GROUP BY 
    local_authorities.name,
    provinces.name, 
    districts.name, 
    actual_budgets.month, 
    actual_budgets.year
ORDER BY
    local_authorities.name,
    actual_budgets.year,
    actual_budgets.month;
''';

expenditure_df=pd.read_sql_query(expenditure_query, engine)

expenditure_df.fillna(0, inplace=True)

# Calculate the total recurrent expenditure budget and actual amounts
expenditure_df['RecurrentExpenditureTotalBudget'] = (
    expenditure_df['PersonalEmolumentBudget'] + expenditure_df['TravelingExpensesBudget'] + expenditure_df['SuppliesRequisitesBudget'] +
    expenditure_df['RepairsMaintenanceofCapitalAssetsBudget'] + expenditure_df['TransportationCommunicationUtilityServiceBudget'] +
    expenditure_df['InterestPaymentsDividendsBudget'] + expenditure_df['GrantsContributionsSubsidiesBudget'] + expenditure_df['PensionsRetirementBenefitsGratuitiesBudget']
)

expenditure_df['RecurrentExpenditureTotalActual'] = (
    expenditure_df['PersonalEmolumentsActual_this_month'] + expenditure_df['TravelingExpensesActual_this_month'] + expenditure_df['SuppliesRequisitesActual_this_month'] +
    expenditure_df['RepairsMaintenanceofCapitalAssetsActual_this_month'] + expenditure_df['TransportationCommunicationUtilityServiceActual_this_month'] +
    expenditure_df['InterestPaymentsDividendsActual_this_month'] + expenditure_df['GrantsContributionsSubsidiesActual_this_month'] + expenditure_df['PensionsRetirementBenefitsGratuitiesActual_this_month']
)

expenditure_df['NonRecurrentExpenditureTotalBudget'] = (
    expenditure_df['CapitalExpenditureBudget'] + expenditure_df['RehabilitationFundBudget'] + expenditure_df['LoanRepaymentsBudget'] +
    expenditure_df['AnyothercapitalexpenditureBudget']
)
expenditure_df['NonRecurrentExpenditureTotalActualUpToThisMonth'] = (
     expenditure_df['CapitalExpenditureActual_this_month'] + expenditure_df['RehabilitationFundActual_this_month'] +
    expenditure_df['LoanRepaymentActual_this_month'] + expenditure_df['AnyothercapitalexpenditureActual_this_month']
)

expenditure_df = expenditure_df[['name','month', 'year','province_name', 'district_name',
       'PersonalEmolumentBudget','PersonalEmolumentsActual_this_month','TravelingExpensesBudget','TravelingExpensesActual_this_month',
       'SuppliesRequisitesBudget','SuppliesRequisitesActual_this_month','RepairsMaintenanceofCapitalAssetsBudget','RepairsMaintenanceofCapitalAssetsActual_this_month',
       'TransportationCommunicationUtilityServiceBudget','TransportationCommunicationUtilityServiceActual_this_month',
       'InterestPaymentsDividendsBudget','InterestPaymentsDividendsActual_this_month','GrantsContributionsSubsidiesBudget',
       'GrantsContributionsSubsidiesActual_this_month','PensionsRetirementBenefitsGratuitiesBudget','PensionsRetirementBenefitsGratuitiesActual_this_month',
       'RecurrentExpenditureTotalBudget','RecurrentExpenditureTotalActual','CapitalExpenditureBudget','CapitalExpenditureActual_this_month',
       'RehabilitationFundBudget','RehabilitationFundActual_this_month','LoanRepaymentsBudget','LoanRepaymentActual_this_month',
       'AnyothercapitalexpenditureBudget','AnyothercapitalexpenditureActual_this_month','NonRecurrentExpenditureTotalBudget','NonRecurrentExpenditureTotalActualUpToThisMonth'
       ]]


# Query the database to retrieve additional data table values into a DataFrame
additional_query='''
SELECT 
    local_authorities.name AS name,
    provinces.name AS province_name,
    districts.name AS district_name,
    actual_budgets.month,
    actual_budgets.year,
    MAX(CASE WHEN revenues.id = 11 THEN annual_budget_details.total_amount ELSE 0 END) AS StampDuty_Budget,
    MAX(CASE WHEN revenues.id = 11 THEN actual_budget_details.total_amount ELSE 0 END) AS StampDuty_ActualThis_month,
    MAX(CASE WHEN revenues.id = 12 THEN annual_budget_details.total_amount ELSE 0 END) AS CourtFines_Budget,
    MAX(CASE WHEN revenues.id = 12 THEN actual_budget_details.total_amount ELSE 0 END) AS CourtFines_ActualThis_month
FROM 
    actual_budgets
INNER JOIN 
    actual_budget_details ON actual_budgets.id = actual_budget_details.actual_budget_id
INNER JOIN 
    local_authorities ON local_authorities.id = actual_budgets.local_authority_id
INNER JOIN 
    districts ON districts.id = local_authorities.district_id
INNER JOIN 
    provinces ON provinces.id = districts.province_id 
INNER JOIN 
    revenues ON revenues.id = actual_budget_details.revenue_id
LEFT JOIN 
    annual_budgets ON annual_budgets.local_authority_id = local_authorities.id AND annual_budgets.year = actual_budgets.year
LEFT JOIN 
    annual_budget_details ON annual_budget_details.annual_budget_id = annual_budgets.id AND annual_budget_details.revenue_id = revenues.id
GROUP BY 
    local_authorities.name,
    provinces.name, 
    districts.name, 
    actual_budgets.month, 
    actual_budgets.year
ORDER BY
    local_authorities.name,
    actual_budgets.year,
    actual_budgets.month;
''';


additional_df=pd.read_sql_query(additional_query, engine)

additional_df.fillna(0, inplace=True)

additional_df = additional_df[['name','month', 'year','province_name', 'district_name',
       'StampDuty_Budget','StampDuty_ActualThis_month','CourtFines_Budget','CourtFines_ActualThis_month'
       ]]

st.header('Local Government Management Information System')
st.markdown('Select Table to View and Filter')

# Function to display tables based on selection
def display_tables(selected_province, selected_district, selected_month, selected_year):
    # Filtered data for Revenue
    filtered_revenue_df = revenue_df[
        (revenue_df['province_name'].isin(selected_province)) &
        (revenue_df['district_name'].isin(selected_district)) &
        (revenue_df['month'] == selected_month) &
        (revenue_df['year'] == selected_year)
    ]

    # Filtered data for Expenditure
    filtered_expenditure_df = expenditure_df[
        (expenditure_df['province_name'].isin(selected_province)) &
        (expenditure_df['district_name'].isin(selected_district)) &
        (expenditure_df['month'] == selected_month) &
        (expenditure_df['year'] == selected_year)
    ]

    # Filtered data for additional data summary
    filtered_additional_df = additional_df[
        (additional_df['province_name'].isin(selected_province)) &
        (additional_df['district_name'].isin(selected_district)) &
        (additional_df['month'] == selected_month) &
        (additional_df['year'] == selected_year)
    ]

    if filtered_revenue_df.empty:
        st.write("No revenue data found for the selected filters.")
    else:
        st.markdown('**Revenue - This Month**')
        st.dataframe(filtered_revenue_df)

    if filtered_expenditure_df.empty:
        st.write("No expenditure data found for the selected filters.")
    else:
        st.markdown('**Expenditure - This Month**')
        st.dataframe(filtered_expenditure_df)

    if filtered_additional_df.empty:
        st.write("No additional data found for the selected filters.")
    else:
        st.markdown('**Additional - This Month**')
        st.dataframe(filtered_additional_df)

province_options = list(revenue_df['province_name'].unique())  # Assuming both dataframes have the same provinces
province_options.insert(0, 'All Provinces')
selected_province = st.selectbox('Select Province', province_options)

if selected_province == 'All Provinces':
    filtered_districts = revenue_df['district_name'].unique()
else:
    # Filter districts based on the selected province
    filtered_districts = revenue_df[revenue_df['province_name'] == selected_province]['district_name'].unique()

district_options = list(filtered_districts)
district_options.insert(0, 'All Districts')
selected_district = st.selectbox('Select District', district_options)

month_options = revenue_df['month'].unique()  # Assuming both dataframes have the same months
year_options = revenue_df['year'].unique()    # Assuming both dataframes have the same years

selected_month = st.selectbox('Select Actual Month', month_options)
selected_year = st.selectbox('Select Year', year_options)

if st.button('Generate'):
    if selected_province == 'All Provinces':
        selected_province = revenue_df['province_name'].unique()
    else:
        selected_province = [selected_province]

    if selected_district == 'All Districts':
        selected_district = revenue_df['district_name'].unique()
    else:
        selected_district = [selected_district]

    display_tables(selected_province, selected_district, selected_month, selected_year)