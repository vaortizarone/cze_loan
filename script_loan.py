import duckdb
import pandas as pd

#%%


duckdb.sql("""

SELECT 
    loan.*,
    account.district_id,
    account.frequency,
    account.date AS account_date,
    disp.disp_id,
    disp.client_id,
    card.type AS card_type
FROM 
    loan.csv AS loan
    INNER JOIN account.csv AS account
        ON loan.account_id = account.account_id
    INNER JOIN disp.csv AS disp
        ON account.account_id = disp.account_id
    LEFT JOIN card.csv AS card
        ON disp.disp_id = card.disp_id

WHERE 
    loan.status IN ('A', 'B')
    AND disp.type = 'OWNER';

""")
#%%
duckdb.sql("""

SELECT
    client_id,
    birth_number,
    CASE
        WHEN CAST(SUBSTRING(CAST(birth_number AS TEXT), 3, 2) AS INTEGER) > 50
            THEN SUBSTRING(CAST(birth_number AS TEXT), 1, 2) || 
                 LPAD(CAST(CAST(SUBSTRING(CAST(birth_number AS TEXT), 3, 2) AS INTEGER) - 50 AS TEXT), 2, '0') || 
                 SUBSTRING(CAST(birth_number AS TEXT), 5, 2)
        ELSE CAST(birth_number AS TEXT)
    END AS birth_date,
    CASE
        WHEN CAST(SUBSTRING(CAST(birth_number AS TEXT), 3, 2) AS INTEGER) > 50 THEN 'Female'
        ELSE 'Male'
    END AS gender
FROM
    client.csv;

""")
#%%


df1 = duckdb.sql(
 """
 SELECT 
           loan.*,
           account.district_id,
           account.frequency,
           account.date AS account_date,
           disp.disp_id,
           disp.client_id,
           card.type AS card_type,
           CASE
           WHEN CAST(SUBSTRING(CAST(client.birth_number AS TEXT), 3, 2) AS INTEGER) > 50
           THEN SUBSTRING(CAST(client.birth_number AS TEXT), 1, 2) || 
             LPAD(CAST(CAST(SUBSTRING(CAST(client.birth_number AS TEXT), 3, 2) AS INTEGER) - 50 AS TEXT), 2, '0') || 
             SUBSTRING(CAST(client.birth_number AS TEXT), 5, 2)
           ELSE CAST(client.birth_number AS TEXT)
           END AS birth_date,
           CASE
           WHEN CAST(SUBSTRING(CAST(client.birth_number AS TEXT), 3, 2) AS INTEGER) > 50 THEN 'Female'
           ELSE 'Male'
           END AS gender
           FROM 
           loan.csv AS loan
           INNER JOIN account.csv AS account
           ON loan.account_id = account.account_id
           INNER JOIN disp.csv AS disp
           ON account.account_id = disp.account_id
           LEFT JOIN card.csv AS card
           ON disp.disp_id = card.disp_id
           INNER JOIN client.csv AS client
           ON disp.client_id = client.client_id
           WHERE 
           loan.status IN ('A', 'B')
           AND disp.type = 'OWNER';
 
 
 """
).df()

#%%

df2 = duckdb.sql("""
SELECT 
    loan.loan_id,
    loan.account_id,
    loan.date ,
    COUNT(trans.trans_id) AS num_transactions,
    SUM(trans.amount) AS total_amount,
    (
        SELECT trans2.balance
        FROM trans.csv AS trans2
        WHERE trans2.account_id = loan.account_id
            AND trans2.date < loan.date
        ORDER BY trans2.date DESC
        LIMIT 1
    ) AS last_balance
FROM 
    loan.csv AS loan
    LEFT JOIN trans.csv AS trans
        ON loan.account_id = trans.account_id
        AND trans.date < loan.date
WHERE
    loan.status IN ('A', 'B')
GROUP BY
    loan.loan_id,
    loan.account_id,
    loan.date;

""").df()

#%%

loan_fin1 = pd.merge(df1, df2, on='loan_id', how='left')
district = pd.read_csv("district.csv", sep=";")

loan_fin2 = pd.merge(loan_fin1, district, left_on='district_id', how='left', right_on="A1")

loan_df = loan_fin2.drop(columns=loan_fin1.columns[[0,1, 7, 10, 11, 15, 16]]).rename(columns={'date_x': 'date_loan'})
#%%

def adjust_year(date_str):
 date_str = str(date_str).zfill(
  6)  # Asegúrate de que la cadena tenga 6 caracteres, completando con ceros a la izquierda si es necesario
 year = int(date_str[:2])
 if year > 24:  # Asumiendo que cualquier año mayor que 24 es del siglo XX
  return f"19{date_str}"
 else:
  return f"20{date_str}"


# Aplicar la función a las columnas de fecha
loan_df['date_loan'] = loan_df['date_loan'].apply(lambda x: adjust_year(str(x)))
loan_df['account_date'] = loan_df['account_date'].apply(lambda x: adjust_year(str(x)))
loan_df['birth_date'] = loan_df['birth_date'].apply(lambda x: adjust_year(str(x)))

date_ref = pd.to_datetime(990101, format='%y%m%d')
#%%

loan_df['date_loan'] = pd.to_datetime(loan_df['date_loan'], format='%Y%m%d')
loan_df['account_date'] = pd.to_datetime(loan_df['account_date'], format='%Y%m%d')
loan_df['birth_date'] = pd.to_datetime(loan_df['birth_date'], format='%Y%m%d')

#%%
loan_df['days_preloan'] = (loan_df['date_loan'] - loan_df['account_date']).dt.days
loan_df['age'] = (date_ref- loan_df['birth_date']).dt.days // 365

loan_df = loan_df.drop(columns=['date_loan', 'account_date', 'birth_date'])
loan_df['card_type'] = loan_df['card_type'].fillna('non_card')
#%%
frequency_mapping = {
    "POPLATEK MESICNE": "Monthly",
    "POPLATEK TYDNE": "Weekly",
    "POPLATEK PO OBRATU": "After Transaction"
}
loan_df['frequency'] = loan_df['frequency'].map(frequency_mapping)

#%%

loan_df = loan_df.rename(columns={'A2': 'district', 'A3': 'region', 'A4': 'num_hab', 'A9': 'num_cit', 'A10': 'ratio_urban', 'A11': 'salary', 'A13': 'emp_rate', 'A14': 'entp_rate', 'A16': 'crimes_rate'})
loan_df = loan_df.drop(['payments', 'A1', 'A5', 'A6', 'A7', 'A8', 'A12', 'A15'],  axis=1)

#%%
#loan_df.to_csv('loan_data_processed.csv', index=False)


