import sqlalchemy
import streamlit as st
import pandas as pd

conn = (
    f"mysql+pymysql://{st.secrets['connections']['my_database']['username']}:"
    f"{st.secrets['connections']['my_database']['password']}@"
    f"{st.secrets['connections']['my_database']['host']}:"
    f"{st.secrets['connections']['my_database']['port']}/"
    f"{st.secrets['connections']['my_database']['database']}"
)
engine = sqlalchemy.create_engine(conn)

# Query the database to retrieve table values into a DataFrame
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
'''

revenue_df = pd.read_sql_query(revenue_query, engine)

revenue_df.fillna(0, inplace=True)

# Ensure 'month' contains valid month values (1-12)
revenue_df['month'] = revenue_df['month'].astype(str).str.zfill(2)  # Ensure two-digit month format
revenue_df = revenue_df[revenue_df['month'].str.match(r'^(0[1-9]|1[0-2])$')]  # Keep only valid months

# Convert month to datetime and extract the month number
revenue_df['month'] = pd.to_datetime(revenue_df['month'], format='%m').dt.month

# Sort the DataFrame by province, district, year, and month to ensure correct cumulative sum
revenue_df.sort_values(by=['name','province_name', 'district_name', 'year', 'month'], inplace=True)

# Calculate the cumulative sum for "Actual (upto this month)"
revenue_df['RateTaxesActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['RateTaxesActual'].cumsum()
revenue_df['RentActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['RentActual'].cumsum()
revenue_df['LicenseActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['LicenseActual'].cumsum()
revenue_df['FeesForServiceActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['FeesForServiceActual'].cumsum()
revenue_df['WarrantCostFinePenaltiesActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['WarrantCostFinePenaltiesActual'].cumsum()
revenue_df['OtherRevenueActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['OtherRevenueActual'].cumsum()
revenue_df['RevenueGrantsAllActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['RevenueGrantsAllActual'].cumsum()
revenue_df['RevenueGrantsOtherActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['RevenueGrantsOtherActual'].cumsum()
revenue_df['CapitalLoansActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['CapitalLoansActual'].cumsum()
revenue_df['CapitalGrantsActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['CapitalGrantsActual'].cumsum()
revenue_df['SaleOfCapitalAssetsActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['SaleOfCapitalAssetsActual'].cumsum()
revenue_df['OtherCapitalReceiptsActual (upto this month)'] = revenue_df.groupby(['name','province_name', 'district_name', 'year'])['OtherCapitalReceiptsActual'].cumsum()

# Calculate the total recurrent revenue budget and actual amounts
revenue_df['RecurrentRevenueTotalBudget'] = (
    revenue_df['RateTaxesBudget'] + revenue_df['RentBudget'] + revenue_df['LicenseBudget'] +
    revenue_df['FeesForServiceBudget'] + revenue_df['WarrantCostFinePenaltiesBudget'] +
    revenue_df['OtherRevenueBudget'] + revenue_df['RevenueGrantsAllBudget'] + revenue_df['RevenueGrantsOtherBudget']
)
revenue_df['RecurrentRevenueTotalActualUpToThisMonth'] = (
    revenue_df['RateTaxesActual (upto this month)'] + revenue_df['RentActual (upto this month)'] + revenue_df['LicenseActual (upto this month)'] +
    revenue_df['FeesForServiceActual (upto this month)'] + revenue_df['WarrantCostFinePenaltiesActual (upto this month)'] +
    revenue_df['OtherRevenueActual (upto this month)'] + revenue_df['RevenueGrantsAllActual (upto this month)'] + revenue_df['RevenueGrantsOtherActual (upto this month)']
)

revenue_df['NonRecurrentRevenueTotalBudget'] = (
    revenue_df['CapitalGrantsBudget'] + revenue_df['CapitalLoansBudget'] + revenue_df['SaleOfCapitalAssetsBudget'] +
    revenue_df['OtherCapitalReceiptsBudget']
)
revenue_df['NonRecurrentRevenueTotalActualUpToThisMonth'] = (
     revenue_df['CapitalGrantsActual (upto this month)'] + revenue_df['CapitalLoansActual (upto this month)'] +
    revenue_df['SaleOfCapitalAssetsActual (upto this month)'] + revenue_df['OtherCapitalReceiptsActual (upto this month)']
)

revenue_df = revenue_df[['name','month', 'year','province_name', 'district_name','RateTaxesBudget','RateTaxesActual (upto this month)',
       'RentBudget','RentActual (upto this month)','LicenseBudget','LicenseActual (upto this month)',
       'FeesForServiceBudget','FeesForServiceActual (upto this month)','WarrantCostFinePenaltiesBudget',
       'WarrantCostFinePenaltiesActual (upto this month)','OtherRevenueBudget','OtherRevenueActual (upto this month)',
       'RevenueGrantsAllBudget','RevenueGrantsAllActual (upto this month)','RevenueGrantsOtherBudget',
       'RevenueGrantsOtherActual (upto this month)','RecurrentRevenueTotalBudget','RecurrentRevenueTotalActualUpToThisMonth',
       'CapitalGrantsBudget','CapitalGrantsActual (upto this month)','CapitalLoansBudget','CapitalLoansActual (upto this month)',
       'SaleOfCapitalAssetsBudget','SaleOfCapitalAssetsActual (upto this month)','OtherCapitalReceiptsBudget',
       'OtherCapitalReceiptsActual (upto this month)','NonRecurrentRevenueTotalBudget','NonRecurrentRevenueTotalActualUpToThisMonth'
       ]]


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



expenditure_df = pd.read_sql_query(expenditure_query, engine)

revenue_df.fillna(0, inplace=True)

# Ensure 'month' contains valid month values (1-12)
expenditure_df['month'] = expenditure_df['month'].astype(str).str.zfill(2)  # Ensure two-digit month format
expenditure_df= expenditure_df[expenditure_df['month'].str.match(r'^(0[1-9]|1[0-2])$')]  # Keep only valid months

# Convert month to datetime and extract the month number
expenditure_df['month'] = pd.to_datetime(expenditure_df['month'], format='%m').dt.month

# Sort the DataFrame by province, district, year, and month to ensure correct cumulative sum
expenditure_df.sort_values(by=['name','province_name', 'district_name', 'year', 'month'], inplace=True)

expenditure_df['PersonalEmolumentsActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['PersonalEmolumentsActual_this_month'].cumsum()
expenditure_df['TravelingExpensesActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['TravelingExpensesActual_this_month'].cumsum()
expenditure_df['SuppliesRequisitesActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['SuppliesRequisitesActual_this_month'].cumsum()
expenditure_df['RepairsMaintenanceofCapitalAssetsActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['RepairsMaintenanceofCapitalAssetsActual_this_month'].cumsum()
expenditure_df['TransportationCommunicationUtilityServiceActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['TransportationCommunicationUtilityServiceActual_this_month'].cumsum()
expenditure_df['InterestPaymentsDividendsActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['InterestPaymentsDividendsActual_this_month'].cumsum()
expenditure_df['GrantsContributionsSubsidiesActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['GrantsContributionsSubsidiesActual_this_month'].cumsum()
expenditure_df['PensionsRetirementBenefitsGratuitiesActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['PensionsRetirementBenefitsGratuitiesActual_this_month'].cumsum()
expenditure_df['CapitalExpenditureActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['CapitalExpenditureActual_this_month'].cumsum()
expenditure_df['RehabilitationFundActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['RehabilitationFundActual_this_month'].cumsum()
expenditure_df['LoanRepaymentActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['LoanRepaymentActual_this_month'].cumsum()
expenditure_df['AnyothercapitalexpenditureActual_this_month(upto this month)'] = expenditure_df.groupby(['name','province_name', 'district_name', 'year'])['AnyothercapitalexpenditureActual_this_month'].cumsum()


# Calculate the total recurrent revenue budget and actual amounts
expenditure_df['RecurrentExpenditureTotalBudget'] = (
    expenditure_df['PersonalEmolumentBudget'] + expenditure_df['TravelingExpensesBudget'] +
expenditure_df['SuppliesRequisitesBudget'] +expenditure_df['RepairsMaintenanceofCapitalAssetsBudget'] +
expenditure_df['TransportationCommunicationUtilityServiceBudget'] + expenditure_df['InterestPaymentsDividendsBudget'] +
expenditure_df['GrantsContributionsSubsidiesBudget'] + expenditure_df['PensionsRetirementBenefitsGratuitiesBudget']
)


expenditure_df['RecurrentExpenditureTotalActualUpToThisMonth'] = (
        expenditure_df['PersonalEmolumentsActual_this_month(upto this month)'] + expenditure_df['TravelingExpensesActual_this_month(upto this month)']+ expenditure_df['SuppliesRequisitesActual_this_month(upto this month)']+
    expenditure_df['RepairsMaintenanceofCapitalAssetsActual_this_month(upto this month)'] +expenditure_df['TransportationCommunicationUtilityServiceActual_this_month(upto this month)'] +
    expenditure_df['InterestPaymentsDividendsActual_this_month(upto this month)']+ expenditure_df['GrantsContributionsSubsidiesActual_this_month(upto this month)']  + expenditure_df['PensionsRetirementBenefitsGratuitiesActual_this_month(upto this month)']
)


expenditure_df['NonRecurrentExpenditureTotalBudget'] = (
    expenditure_df['CapitalExpenditureBudget'] + expenditure_df['RehabilitationFundBudget'] + expenditure_df['LoanRepaymentsBudget'] +
    expenditure_df['AnyothercapitalexpenditureBudget']
)
expenditure_df['NonRecurrentExpenditureTotalActualUpToThisMonth'] = (
        expenditure_df['CapitalExpenditureActual_this_month(upto this month)'] + expenditure_df['RehabilitationFundActual_this_month(upto this month)']  +
    expenditure_df['LoanRepaymentActual_this_month(upto this month)'] +expenditure_df['AnyothercapitalexpenditureActual_this_month(upto this month)']
)

expenditure_df = expenditure_df[['name','month', 'year','province_name', 'district_name','PersonalEmolumentBudget','PersonalEmolumentsActual_this_month(upto this month)',
       'TravelingExpensesBudget','TravelingExpensesActual_this_month(upto this month)','SuppliesRequisitesBudget','SuppliesRequisitesActual_this_month(upto this month)',
       'RepairsMaintenanceofCapitalAssetsBudget','RepairsMaintenanceofCapitalAssetsActual_this_month(upto this month)','TransportationCommunicationUtilityServiceBudget',
       'TransportationCommunicationUtilityServiceActual_this_month(upto this month)','InterestPaymentsDividendsBudget','InterestPaymentsDividendsActual_this_month(upto this month)',
       'GrantsContributionsSubsidiesBudget','GrantsContributionsSubsidiesActual_this_month(upto this month)','PensionsRetirementBenefitsGratuitiesBudget',
       'PensionsRetirementBenefitsGratuitiesActual_this_month(upto this month)','RecurrentExpenditureTotalBudget','RecurrentExpenditureTotalActualUpToThisMonth',
       'CapitalExpenditureBudget','CapitalExpenditureActual_this_month(upto this month)','RehabilitationFundBudget','RehabilitationFundActual_this_month(upto this month)',
       'LoanRepaymentsBudget','LoanRepaymentActual_this_month(upto this month)','AnyothercapitalexpenditureBudget',
       'AnyothercapitalexpenditureActual_this_month(upto this month)','NonRecurrentExpenditureTotalBudget','NonRecurrentExpenditureTotalActualUpToThisMonth'
       ]]

# Query the database to retrieve table values into a DataFrame for additional data
additional_query='''
SELECT 
    local_authorities.name AS name,
    provinces.name AS province_name,
    districts.name AS district_name,
    actual_budgets.month,
    actual_budgets.year,
    MAX(CASE WHEN revenues.id = 11 THEN annual_budget_details.total_amount ELSE 0 END) AS StampDuty_Budget,
    MAX(CASE WHEN revenues.id = 11 THEN actual_budget_details.total_amount ELSE 0 END) AS StampDuty_Actual,
    MAX(CASE WHEN revenues.id = 12 THEN annual_budget_details.total_amount ELSE 0 END) AS CourtFines_Budget,
    MAX(CASE WHEN revenues.id = 12 THEN actual_budget_details.total_amount ELSE 0 END) AS CourtFines_Actual
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


additional_df = pd.read_sql_query(additional_query, engine)

revenue_df.fillna(0, inplace=True)

# Ensure 'month' contains valid month values (1-12)
additional_df['month'] = additional_df['month'].astype(str).str.zfill(2)  # Ensure two-digit month format
additional_df = additional_df[additional_df['month'].str.match(r'^(0[1-9]|1[0-2])$')]  # Keep only valid months

# Convert month to datetime and extract the month number
additional_df['month'] = pd.to_datetime(additional_df['month'], format='%m').dt.month

# Sort the DataFrame by province, district, year, and month to ensure correct cumulative sum
additional_df.sort_values(by=['name','province_name', 'district_name', 'year', 'month'], inplace=True)



# Calculate the cumulative sum for "Actual (upto this month)"
additional_df['StampDuty_Actual(upto this month)'] = additional_df.groupby(['name','province_name', 'district_name', 'year'])['StampDuty_Actual'].cumsum()
additional_df['CourtFines_Actual(upto this month)'] = additional_df.groupby(['name','province_name', 'district_name', 'year'])['CourtFines_Actual'].cumsum()



additional_df = additional_df[['name','month', 'year','province_name','district_name','StampDuty_Budget','StampDuty_Actual(upto this month)','CourtFines_Budget','CourtFines_Actual(upto this month)']]



st.header('Local Government Management Information System')
st.markdown('Select Table to View and Filter')

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