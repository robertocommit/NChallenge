# Lead Analytics Engineer Challenge
Hello, my name is Roberto, and I am an experienced analytics engineer. 
In this document, I present my solution for the provided challenge, which aims to analyze transaction data and extract valuable insights. 

## Table of Contents
<!-- vim-markdown-toc GFM -->

* [Summary](#summary)
* [1. Data Upload](#1-data-upload)
* [2. SQL Query with Window Function](#2-sql-query-with-window-function)
* [3. Python Data Aggregation](#3-python-data-aggregation)
  * [Initial Query](#initial-query)
    * [Result](#result)
  * [Python Script](#python-script)
    * [Result](#result-1)
* [Next Steps](#next-steps)

<!-- vim-markdown-toc -->

## Summary

In the first task, we designed an SQL query to compute a DWH table for transactions. 
Our query calculates the number of transactions each user had within the previous seven days using a window function. 

For the second task, we were asked to compute the equivalent result of a specific SQL query using Python, without any external packages.
This script successfully provides the expected output, offering an alternative approach to the SQL query.

## 1. Data Upload

First, we created a script to upload the data from the CSV files `transactions.csv` and `users.csv` into a PostgreSQL database. 

The script 
- establishes a connection with the database
- creates the necessary tables 
- inserts the data from the CSV files into the corresponding tables

<details>
  <summary>Here's the script we used to achieve this:</summary>

```python
import os
import psycopg2
import pandas as pd

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ['DB_PORT']
)
cursor = conn.cursor()

# Load the transactions and users CSV files into Pandas DataFrames
transactions = pd.read_csv('transactions.csv')
users = pd.read_csv('users.csv')

# Create a cursor object to interact with the database
cur = conn.cursor()

# Create the transactions and users tables in the database
cur.execute("""
CREATE TABLE transactions (
    transaction_id UUID,
    date DATE,
    user_id UUID,
    is_blocked BOOL,
    transaction_amount INTEGER,
    transaction_category_id INTEGER
);
""")
cur.execute("""
CREATE TABLE users (
    user_id UUID,
    is_active BOOLEAN
)
""")

# Insert the transactions data into the transactions table
batch_size = 1000

# Insert the transactions data into the transactions table in batches
for i in range(0, len(transactions), batch_size):
    print("Inserting transactions batch {} to {}".format(i, i + batch_size))
    batch = transactions.iloc[i:i+batch_size]
    rows = [tuple(x) for x in batch.to_numpy()]
    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in rows)
    cur.execute("INSERT INTO transactions (transaction_id, date, user_id, is_blocked, transaction_amount, transaction_category_id) VALUES " + args_str)
    conn.commit()

# Insert the users data into the users table
for index, row in users.iterrows():
    print("Inserting user {}".format(row['user_id']))
    cur.execute("""
    INSERT INTO users (user_id, is_active)
    VALUES (%s, %s)
    """, (
        row['user_id'],
        row['is_active']
    ))

# Commit the changes to the database
conn.commit()

# Close the cursor and connection objects
cur.close()
conn.close()
```
</details>

## 2. SQL Query with Window Function

Next, we crafted an SQL query that leverages a window function to calculate the number of transactions each user had within the previous seven days.
This feature of SQL allows us to perform calculations across a set of rows related to the current row.

Here's the SQL query we used:
```sql
SELECT
    transaction_id,
    user_id,
    date,
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY date
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS no_txn_last_7days
FROM
    transactions
ORDER BY
    user_id, date;
```

To optimize this query, a query planner should consider factors such as:
- indexing
- partition 
- parallelism

## 3. Python Data Aggregation

### Initial Query
Finally, we were asked to compute the equivalent result of the following SQL query using Python, without using any external packages:

Below a correct version of the query (the one provided has a small issue):
```sql
SELECT
    t.transaction_category_id,
    sum(t.transaction_amount) AS sum_amount,
    count(DISTINCT t.user_id) AS num_users
FROM
    transactions t
JOIN
    users u USING (user_id)
WHERE
    t.is_blocked IS FALSE
    AND u.is_active IS TRUE
GROUP BY
    t.transaction_category_id
ORDER BY
    1 DESC;
```

#### Result
The query produced the following result:

|transaction_category_id|sum_amount|num_users|
|-----------------------|----------|---------|
|0                      |2967      |63       |
|1                      |4424      |79       |
|2                      |4668      |79       |
|3                      |4096      |73       |
|4                      |5307      |93       |
|5                      |4449      |78       |
|6                      |4113      |84       |
|7                      |4701      |83       |
|8                      |4419      |84       |
|9                      |3795      |79       |
|10                     |3819      |70       |

### Python Script

We created a Python script named `transactions_aggregation.py` that:
1. reads the data from the CSV files
2. filters the transactions based on the specified conditions
3. aggregates the data according to the transaction categories
4. sorts the results by the total transaction amount and prints them to the stdout

<details>
  <summary>Here's the script we used to achieve this:</summary>

```python
import csv
from collections import defaultdict

# Read users data
users = {}
with open('users.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        users[row['user_id']] = row['is_active'].lower() == 'true'

# Read transactions data and filter based on conditions
filtered_transactions = []
with open('transactions.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['is_blocked'].lower() == 'false' and users.get(row['user_id']):
            filtered_transactions.append(row)

# Aggregate data based on transaction_category_id
aggregated_data = defaultdict(lambda: {'sum_amount': 0, 'user_ids': set()})
for transaction in filtered_transactions:
    transaction_category_id = transaction['transaction_category_id']
    aggregated_data[transaction_category_id]['sum_amount'] += float(transaction['transaction_amount'])
    aggregated_data[transaction_category_id]['user_ids'].add(transaction['user_id'])

# Process aggregated data for output
result = []
for transaction_category_id, data in aggregated_data.items():
    result.append({
        'transaction_category_id': transaction_category_id,
        'sum_amount': data['sum_amount'],
        'num_users': len(data['user_ids'])
    })

# Sort and print the result
result.sort(key=lambda x: x['transaction_category_id'])
for row in result:
    print(f"{row['transaction_category_id']}, {row['sum_amount']:.2f}, {row['num_users']}")
```

</details>

#### Result
The script produced the following result:

|transaction_category_id|sum_amount|num_users|
|-----------------------|----------|---------|
|0                      | 2965.62  | 63      |
|1                      | 4419.24  | 79      |
|2                      | 4669.46  | 79      |
|3                      | 4094.86  | 73      |
|4                      | 5300.95  | 93      |
|5                      | 4446.94  | 78      |
|6                      | 4117.48  | 84      |
|7                      | 4701.69  | 83      |
|8                      | 4421.53  | 84      |
|9                      | 3797.42  | 79      |
|10                     | 3815.09  | 70      |

## Next Steps

As potential next steps I would reccomend:

1. **Data Validation**: Ensure data consistency across both methods to reduce discrepancies.
2. **Testing**: Implement comprehensive testing to ensure the correctness and reliability of our solutions.
3. **Optimization**: Improve efficiency by exploring indexing, partitioning, and parallelism techniques.
4. **Documentation**: Clearly explain our approaches, assumptions, and limitations for better understanding and reproducibility.
5. **Error Handling**: The current scripts and queries have not been coded including a proper way to manage errors.

