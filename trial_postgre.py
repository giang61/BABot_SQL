import os

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv(find_dotenv())

# Set OpenAI API key
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Get database connection parameters
db_type = os.getenv('DB_TYPE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
hostname = os.getenv('DB_HOSTNAME')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# Construct the database URI
uri = f"{db_type}://{username}:{password}@{hostname}:{port}/{database}"

# 1. Connect to PostgreSQL
engine = create_engine(uri)

# 2. Create a table from Appro limited in time
select_query = """
SELECT cetab, ltie, qterec1, daterec
FROM dafacd
WHERE daterec >= '2024-01-02' 
AND daterec <= '2024-08-31';
"""

# Execute the query and store the result in a DataFrame
df = pd.read_sql_query(select_query, engine)
print("Data from dafacd:")
print(df)

# Create an in-memory SQLite database and write the DataFrame to it
sqlite_engine = create_engine(r'sqlite:///C:/Users/bob/PycharmProjects/BABot/sqlitedb/temp.db', echo=False)
df.to_sql('Appro', sqlite_engine, index=False, if_exists='replace')

# 3. Create a 2nd table with the names of the Group suppliers
select_query = """
SELECT DISTINCT lvniv1
FROM dhierfou
WHERE ltypehier = 'GROUPE INDUSTRIEL/PRODUCTEURS';
"""

# Execute the query and store the result in a DataFrame
df = pd.read_sql_query(select_query, engine)
print("Group suppliers from dhierfou:")
print(df)
df.to_sql('Groupe', sqlite_engine, index=False, if_exists='replace')

# 4. Create a 3rd table with the Appro table filtered by Group suppliers
select_query = """
SELECT Appro.cetab, Appro.ltie, Appro.qterec1, Appro.daterec
FROM Appro
INNER JOIN Groupe
ON Appro.ltie LIKE Groupe.lvniv1 || '%';
"""

# Execute the query and store the result in a DataFrame using the SQLite engine
df = pd.read_sql_query(select_query, sqlite_engine)
print("Appro data filtered by Group suppliers:")
print(df)
df.to_sql('InnerJoin', sqlite_engine, index=False, if_exists='replace')

# 4. Create a 4th table with the InnerJoin table filtered by Group suppliers
select_query = """
SELECT cetab, SUBSTR(ltie, 1, 7) AS ltie_prefix, SUM(qterec1) AS total_weight
FROM InnerJoin
GROUP BY cetab, ltie_prefix
ORDER BY total_weight DESC;
"""
# Execute the query and store the result in a DataFrame using the SQLite engine
df = pd.read_sql_query(select_query, sqlite_engine)
print("Appro data consolidated by Suppliers groups:")
print(df)
df.to_sql('ApproGroup', sqlite_engine, index=False, if_exists='replace')

"""
db = SQLDatabase(sqlite_engine)
llm = OpenAI(verbose = True, api_key=OPENAI_API_KEY)

def database_response(query_text, llm = llm, db=db):
    db_chain = SQLDatabaseChain.from_llm(llm, db, verbose = True)
    # use the following for very large DB
    #db_chain = SQLDatabaseSequentialChain.from_llm(llm=llm, db=db, verbose=True, use_query_checker=True, top_k=1)
    res = db_chain.invoke(query_text)
    return res

database_response("Quels sont les 5 fournisseurs avec le plus de poids en mars 2024?")
#database_response("Montrer les poids mensuels par fournisseur en 6 colonnes, de janvier à juin")
"""