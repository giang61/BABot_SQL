import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import streamlit as st
from langchain_community.llms import OpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from csv_to_sqllite import csv_to_sqlite  # Import the function

#fonctions pour gérer l'historique des conversations
def load_conversation_history():
    """Load conversation history from a file."""
    try:
        with open(CONVERSATION_FILE_PATH, "r") as file:
            return file.readlines()
    except FileNotFoundError:
        return []

def save_conversation_history(user_input, bot_response):
    """Save conversation history to a file."""
    with open(CONVERSATION_FILE_PATH, "a") as file:
        file.write(f"user: {user_input}\n")
        file.write(f"BABot: {bot_response}\n")

# Set the page configuration to wide mode
st.set_page_config(page_title="Analysez votre base de données avec BABot_SQL", layout="wide")

# Load environment variables
load_dotenv(find_dotenv())
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
llm = OpenAI(temperature=0.5, api_key=OPENAI_API_KEY)
dburi = ""
CONVERSATION_FILE_PATH = "conversation_history.txt"


if not OPENAI_API_KEY:
    st.error("Clé OpenAI API Key introuvable. Veuillez vérifier votre fichier .env")
else:
    os.environ['OPENAI_API_KEY'] = OPENAI_API_KEY

st.title("Analysez votre base excel/csv/sql avec BABot_SQL")

# Create two columns for horizontal layout
col1, col2 = st.columns([0.75, 0.75])

# File uploader for the CSV file in the first column
with col1:
    uploaded_csv_file = st.file_uploader("Choisissez un fichier CSV à analyser", type="csv")

if uploaded_csv_file is not None:
    def initialize_paths():
        # Reinitialize paths within the function scope
        csv_temp_path = "temp_csv_upload.csv"
        db_temp_path = "temp_converted_db.sqlite"

        # Remove old database file if it exists
        if os.path.exists(db_temp_path):
            os.remove(db_temp_path)

        return csv_temp_path, db_temp_path

    # Initialize paths
    csv_temp_path, db_temp_path = initialize_paths()

    # Proceed with the rest of your logic (e.g., saving and processing the file)
    with open(csv_temp_path, "wb") as f:
        f.write(uploaded_csv_file.getbuffer())

    # Button to trigger the CSV to SQLite conversion
    if st.button("Convertir CSV en base de données SQLite"):
        # Run the csv_to_sqlite function to convert CSV to SQLite
        csv_to_sqlite(csv_temp_path, db_temp_path)
        st.success("Fichier CSV converti en base de données SQLite avec succès!")
        dburi = "sqlite:///"+db_temp_path
        # Connect to the uploaded SQLite database
        try:
            conn = sqlite3.connect(db_temp_path)
            cursor = conn.cursor()

            # Get the list of tables in the database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            conn.close()

            if tables:
                # Allow the user to select a table to display
                table_names = [table[0] for table in tables]
                selected_table = st.selectbox("Sélectionnez une table à afficher:", table_names)

                # Display the first few rows of the selected table
                conn = sqlite3.connect(db_temp_path)
                df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 5", conn)
                st.write(f"Affichage des 5 premières lignes de la table '{selected_table}':")
                st.dataframe(df)
                conn.close()

                # Update the database URI to use the uploaded database
                db = SQLDatabase.from_uri(dburi)
                db_chain = SQLDatabaseChain(llm=llm, database=db, verbose=True)
            else:
                st.warning("Aucune table trouvée dans la base de données.")
        except Exception as e:
            st.error(f"Erreur lors de la lecture de la base de données: {e}")

# File uploader for the SQLite database in the second column
with col2:
    uploaded_sqlite_file = st.file_uploader("Ou choisissez une base de données SQLite à analyser", type="sqlite")

if uploaded_sqlite_file is not None:
    # Save the uploaded SQLite file to a temporary location
    temp_db_path = "temp_uploaded_db.sqlite"
    with open(temp_db_path, "wb") as f:
        f.write(uploaded_sqlite_file.getvalue())

    st.success("Fichier SQLite ajouté avec succès!")

    # Connect to the uploaded SQLite database
    try:
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Get the list of tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        conn.close()

        if tables:
            # Allow the user to select a table to display
            table_names = [table[0] for table in tables]
            selected_table = st.selectbox("Sélectionnez une table à afficher:", table_names)

            # Display the first few rows of the selected table
            conn = sqlite3.connect(temp_db_path)
            df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 5", conn)
            st.write(f"Affichage des 5 premières lignes de la table '{selected_table}':")
            st.dataframe(df)
            conn.close()

            # Update the database URI to use the uploaded database
            dburi = f"sqlite:///{temp_db_path}"
            db = SQLDatabase.from_uri(dburi)
            db_chain = SQLDatabaseChain(llm=llm, database=db, verbose=True)
        else:
            st.warning("Aucune table trouvée dans la base de données.")

    except Exception as e:
        st.error(f"Erreur lors de la lecture de la base de données: {e}")

# Input field for the user query
st.subheader("Votre requête personnalisée sous forme de question")
user_query = st.text_input("Posez votre question", placeholder="Exemple: Quel est le nombre total de ventes?")
query_button = st.button("Exécuter requête")



# Execute the query when the button is clicked
if query_button:
    if dburi == "":
        st.warning("Aucune base de données sélectionnée.")
    else:
        with st.spinner("En cours d'exécution..."):
            try:
                result = db_chain.run(user_query)
                # Display results based on format
                st.write(result)
                # Format messages for history
                user_input_formatted = user_query.replace("\n", ":newligne:")
                bot_response_formatted = result.replace("\n", ":newligne:")

                # Update conversation history
                st.session_state.conversation_history.append(f"user: {user_input_formatted}")
                st.session_state.conversation_history.append(bot_response_formatted)


                # Save to file
                save_conversation_history(user_input_formatted, bot_response_formatted)
            except Exception as e:
                st.error(f"Echec de l'exécution: {e}")


# Initialize session state for conversation history
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []
if 'uploaded_file_content' not in st.session_state:
    st.session_state.uploaded_file_content = ""

st.session_state.conversation_history = load_conversation_history()
# Display conversation history
for item in reversed(st.session_state.conversation_history):
    item_display = item.replace(":newligne:", "\n")
    if "user" in item:
        st.info(item_display.replace("user: ", ""))
    elif "BABot" in item:
        st.success(item_display.replace("BABot: ", ""))
