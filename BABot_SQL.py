import os
import sqlite3
import openai
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.prompts import PromptTemplate
from csv_to_sqllite import csv_to_sqlite  # Import the function
from excel_to_sqllite import excel_to_sqlite  # Function that converts an xlsx file to an sqlite file

# Functions for managing conversation history
def load_conversation_history():
    """Load conversation history from a file."""
    try:
        with open(CONVERSATION_FILE_PATH, "r") as file:
            return file.readlines()
    except FileNotFoundError:
        return []

def save_conversation_history(user_input, bot_response):
    """Save conversation history to a file."""
    user_input = user_input.replace("\n", ":newligne:")
    bot_response = bot_response.replace("\n", ":newligne:").replace("Answer:", "")
    with open(CONVERSATION_FILE_PATH, "a") as file:
        file.write(f"user: {user_input}\n")
        file.write(f"BABot: {bot_response}\n")

def initialize_paths(uploaded_file):
    """Initialize file paths for the uploaded file."""
    file_name_formatted = os.path.splitext(os.path.basename(uploaded_file.name))[0].replace(" ", "_").replace(
        "-", "_").replace(".", "_")
    if uploaded_file.name.endswith(".csv"):
        csv_path = f"data/temp/temp_{file_name_formatted}.csv"
    elif uploaded_file.name.endswith(".xlsx"):
        csv_path = f"data/temp/temp_{file_name_formatted}.xlsx"
    db_path = f"data/temp/temp_{file_name_formatted}.sqlite"
    return csv_path, db_path

# Set the page configuration to wide mode
st.set_page_config(page_title="Analysez votre base de données avec BABot_SQL", layout="wide")

# Load environment variables
load_dotenv(find_dotenv())
openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialize LLM
llm = ChatOpenAI(model="gpt-4o", temperature=0, openai_api_key=openai.api_key,
    max_tokens=4000)


# Initialize session state for persistent variables
if 'dburi' not in st.session_state:
    st.session_state.dburi = ""

if 'uploaded_sql' not in st.session_state:
    st.session_state.uploaded_sql = ""

if "table_names" not in st.session_state:
    st.session_state.table_names = []

CONVERSATION_FILE_PATH = "conversation_history.txt"

if not openai.api_key:
    st.error("Clé OpenAI API Key introuvable. Veuillez vérifier votre fichier .env")
else:
    os.environ['OPENAI_API_KEY'] = openai.api_key

st.title("Analysez votre base excel/csv/sql avec BABot_SQL")

# File uploader for the CSV/Excel file
uploaded_csv_xlsx_file = st.file_uploader("Choisissez un fichier (CSV ou Excel) à analyser", type=["csv", "xlsx"])

if uploaded_csv_xlsx_file is not None:
    # Initialize paths
    csv_temp_path, db_temp_path = initialize_paths(uploaded_csv_xlsx_file)
    # Save uploaded file temporarily
    with open(csv_temp_path, "wb") as f:
        f.write(uploaded_csv_xlsx_file.getbuffer())

    convert_button = st.button("Convertir CSV en base de données SQLite")
    # Button to trigger the CSV/Excel to SQLite conversion
    if convert_button:
        # Ensure the database file is freshly created by removing any existing one
        if os.path.exists(db_temp_path):
            os.remove(db_temp_path)  # Delete the existing SQLite file

        if uploaded_csv_xlsx_file.name.endswith(".csv"):
            csv_to_sqlite(csv_temp_path, db_temp_path)
        else:
            excel_to_sqlite(csv_temp_path, db_temp_path)
        st.success("Fichier converti en base de données SQLite avec succès!")

        st.session_state.uploaded_sql = db_temp_path
        st.session_state.dburi = f"sqlite:///{db_temp_path}"

        db = SQLDatabase.from_uri(st.session_state.dburi)
        try:
            conn = sqlite3.connect(db_temp_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            st.session_state.table_names = [table[0] for table in tables] if tables else []
            conn.close()
        except Exception as e:
            st.error(f"Erreur lors de la lecture de la base de données: {e}")

# File uploader for SQLite database
uploaded_sqlite_file = st.file_uploader("Ou choisissez une base de données SQLite à analyser", type="sqlite")

if uploaded_sqlite_file is not None:
    temp_db_path = "data/temp/temp_uploaded_db.sqlite"

    # Ensure any existing SQLite file is removed before saving the new one
    if os.path.exists(temp_db_path):
        try:
            os.remove(temp_db_path)
        except Exception as e:
            st.error(f"Erreur lors de la suppression de l'ancien fichier SQLite: {e}")
            st.stop()  # Stop execution if the file can't be removed

    with open(temp_db_path, "wb") as f:
        f.write(uploaded_sqlite_file.getvalue())
    st.success("Fichier SQLite ajouté avec succès!")
    st.session_state.uploaded_sql = temp_db_path
    st.session_state.dburi = f"sqlite:///{temp_db_path}"

    try:
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        st.session_state.table_names = [table[0] for table in tables] if tables else []
        conn.close()
    except Exception as e:
        st.error(f"Erreur lors de la lecture de la base de données: {e}")

# Display tables
if st.session_state.table_names:
    selected_table = st.selectbox("Sélectionnez une table à afficher:", st.session_state.table_names)
    conn = sqlite3.connect(st.session_state.uploaded_sql)
    df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 5", conn)
    st.write(f"Affichage des 5 premières lignes de la table '{selected_table}':")
    st.dataframe(df)
    conn.close()

# Input field for the user query
st.subheader("Votre requête personnalisée sous forme de question")
user_query = st.text_input("Posez votre question", placeholder="Exemple: Quel est le nombre total de ventes?")
query_button = st.button("Exécuter requête")

if query_button:
    if not st.session_state.dburi:
        st.warning("Aucune base de données sélectionnée.")
    else:
        with st.spinner("En cours d'exécution..."):
            try:
                db = SQLDatabase.from_uri(st.session_state.dburi)
                db_chain = SQLDatabaseChain.from_llm(
                    llm=llm,
                    db=db,
                    verbose=True,
                    prompt=PromptTemplate(
                        input_variables=["input", "table_info"],
                        template=(
                            "You are a highly skilled SQL expert tasked with answering questions using database information. "
                            "Given the user's question: '{input}' and the database schema details: '{table_info}', follow these precise instructions:\n\n"
                            "1. **Generate the most appropriate SQL query** to answer the user's question based on the given schema. Ensure the query is optimized and accurate.\n"
                            "2. Handle errors gracefully, providing actionable suggestions if needed.\n"
                            "3. **Do not include any Markdown formatting** at the beginning and end of the sql query (e.g., ```sql).\n"
                            "4. Use double quotes for any column or table names that contain special characters or spaces.\n"
                            "5. Do not provide any explanations. Just the SQL query code.\n"
                        )
                    )
                )

                query = db_chain.invoke(user_query)

                conn = sqlite3.connect(st.session_state.dburi.split("sqlite:///")[1])  # Extract file path
                result = pd.read_sql_query(query['result'], conn)

                conn.close()

                result_prompt = PromptTemplate(
                    input_variables=["input", "query_result"],
                    template=(
                        "You are a highly skilled SQL expert tasked with answering questions using database information. "
                        "Given the user's question: '{input}' and the query result: '{query_result}', follow these precise instructions:\n\n"
                        "1. **Generate the answer in French** based on the user's question and the query result.\n"
                        "2. Make sure the answer is always in proper French and reflects the query result.\n"
                    )
                )

                # Step 3: Format the prompt with the user input and query result
                formatted_prompt = result_prompt.format(
                    input=user_query,
                    query_result=result.to_string(index=False)
                )
                response = llm.invoke(formatted_prompt)
                # Access only the 'content' part of the response
                french_response = response.content if hasattr(response, "content") else response.get("content", response)
                # Step 5: Display the result in French
                st.write(french_response)

                save_conversation_history(user_query, french_response)
            except Exception as e:
                st.error(f"Echec de l'exécution: {e}")

# Display conversation history
st.session_state.conversation_history = load_conversation_history()
for item in reversed(st.session_state.conversation_history):
    item_display = item.replace(":newligne:", "\n")
    if "user" in item:
        st.info(item_display.replace("user: ", ""))
    elif "BABot" in item:
        st.success(item_display.replace("BABot: ", ""))
