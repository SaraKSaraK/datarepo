# Importing necessary libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2

# Reading the dataset
file_path = r'C:\Users\Damjan i Sara\Desktop\archive (1)\movies.csv'
df = pd.read_csv(file_path, encoding='mac_roman', delimiter=';')

# Displaying descriptive statistics
print("Descriptive statistics:")
print(df.describe())

# Exploratory data analysis
plt.figure(figsize=(10, 6))
plt.hist(df['Vote_average'], bins=20, color='skyblue', edgecolor='black')
plt.title('Distribution of Movie Ratings')
plt.xlabel('Vote Average')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

# Handling missing values
missing_values = df.isnull().sum()
print("Missing values per column:")
print(missing_values)

# Data preprocessing
df['Popularity'] = df['Popularity'].str.replace('.', '').astype(float)
df['First_air_date'] = pd.to_datetime(df['First_air_date'])
df['First_air_date_ordinal'] = (df['First_air_date'] - df['First_air_date'].min()).dt.days
df.drop(columns=['First_air_date'], inplace=True)

# Visualizing correlations
correlation_matrix = df[['Popularity', 'Vote_average', 'Vote_count', 'First_air_date_ordinal']].corr()
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
plt.title('Correlation Matrix')
plt.show()

# Connecting to PostgreSQL database
dbname = "Projectdb"
user = "postgres"
password = "123"
host = "localhost"
port = 5433
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Creating table in the database
schema = ", ".join([f"{column} {dtype}" for column, dtype in zip(df.columns, df.dtypes)])
create_table_query = f"CREATE TABLE IF NOT EXISTS movies ({schema});"
with conn.cursor() as cursor:
    cursor.execute(create_table_query)
    conn.commit()

# Inserting data into the database
sql_query = """
    INSERT INTO movies (column1, id, name, popularity, vote_average, vote_count, first_air_date_ordinal) 
    VALUES (%(column1)s, %(id)s, %(name)s, %(popularity)s, %(vote_average)s, %(vote_count)s, %(first_air_date_ordinal)s)
"""
cur = conn.cursor()
for index, row in df.iterrows():
    cur.execute(sql_query, {
        'column1': row['column1'],
        'id': row['id'],
        'name': row['name'],
        'popularity': row['popularity'],
        'vote_average': row['vote_average'],
        'vote_count': row['vote_count'],
        'first_air_date_ordinal': row['first_air_date_ordinal']
    })
conn.commit()
cur.close()
conn.close()