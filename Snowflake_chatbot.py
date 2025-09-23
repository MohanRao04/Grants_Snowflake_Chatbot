import streamlit as st
import json
import pandas as pd
import snowflake.connector
import traceback
import os
from dotenv import load_dotenv

# -------------------------
# Load environment variables
# -------------------------
load_dotenv("SECRETS.env")  # Ensure this matches your env filename

# Access environment variables
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SEMANTIC_MODEL_FILE = os.getenv("SEMANTIC_MODEL")

# -------------------------
# Streamlit config
# -------------------------
st.set_page_config(page_title="Cortex AI Assistant", layout="wide")
st.title("🤖 Cortex AI Assistant")
st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# -------------------------
# Check environment variables
# -------------------------
try:
    st.write("DEBUG: Environment Variables Loaded ✅")
    st.write("User:", SNOWFLAKE_USER)
    st.write("Account:", SNOWFLAKE_ACCOUNT)
    st.write("Database:", SNOWFLAKE_DATABASE)
    st.write("Schema:", SNOWFLAKE_SCHEMA)
    st.write("Semantic Model File:", SEMANTIC_MODEL_FILE)

    if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE,
                SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SEMANTIC_MODEL_FILE]):
        st.error("❌ One or more Snowflake environment variables are missing.")
        st.stop()

except Exception as e:
    st.error(f"❌ Error loading environment variables: {e}")
    st.stop()

# -------------------------
# Connect to Snowflake
# -------------------------
try:
    @st.cache_resource
    def get_conn():
        return snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )

    conn = get_conn()
    st.success("✅ Connected to Snowflake!")

except Exception as e:
    st.error(f"❌ Connection to Snowflake failed: {e}")
    conn = None

# -------------------------
# Run query in Snowflake
# -------------------------
def run_snowflake_query(query: str):
    if not conn:
        st.error("❌ No active Snowflake connection.")
        return None
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        cur.close()
        st.write("DEBUG: Query executed successfully ✅")
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"❌ SQL Execution Error: {e}")
        return None

# -------------------------
# Fallback: Plain Text Completion
# -------------------------
def complete_text(prompt: str, model="mistral-large"):
    if not conn:
        st.error("❌ No active connection.")
        return None
    try:
        safe_prompt = prompt.replace("'", "''")
        q = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{safe_prompt}') AS response"
        cur = conn.cursor()
        cur.execute(q)
        row = cur.fetchone()
        cur.close()
        response = row[0] if row else None
        st.write("DEBUG: Fallback text completion response:", response)
        return response
    except Exception as e:
        st.error(f"❌ COMPLETE function error: {e}")
        return None

# -------------------------
# MAIN FUNCTION — TEXT TO SQL USING CORTEX ANALYST
# -------------------------
def generate_sql_from_cortex(user_query: str):
    if not conn:
        st.error("❌ No active connection.")
        return None

    try:
        safe_query = user_query.replace("'", "''")
        safe_model = SEMANTIC_MODEL_FILE.replace("'", "''")

        cortex_call_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE_ANALYST(
          'mistral-large',
          '{safe_model}',
          '{safe_query}'
        ) AS response;
        """

        st.write("DEBUG: Sending to Cortex COMPLETE_ANALYST:")
        st.code(cortex_call_sql, language="sql")

        cur = conn.cursor()
        cur.execute(cortex_call_sql)
        row = cur.fetchone()
        cur.close()

        if not row or not row[0]:
            st.warning("⚠️ Cortex returned empty response.")
            return None

        st.write("DEBUG: Cortex SQL Response Received ✅")
        st.code(row[0], language="sql")
        return row[0].strip()

    except Exception as e:
        st.error("❌ Cortex SQL generation failed: " + str(e))
        st.text(traceback.format_exc())
        return None

# -------------------------
# Main App
# -------------------------
def main():
    st.sidebar.header("🔍 Ask Cortex Anything")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    query = st.chat_input("Ask your question...")

    if not query:
        return

    st.markdown(f"**You asked:** {query}")

    with st.spinner("🧠 Thinking..."):
        sql = generate_sql_from_cortex(query)

        if sql:
            st.markdown("### 🛠️ Generated SQL:")
            st.code(sql, language="sql")

            results = run_snowflake_query(sql)
            st.markdown("### 📊 Query Results:")

            if results is not None and not results.empty:
                st.dataframe(results)
            else:
                st.warning("⚠️ No rows returned from the query.")
        else:
            # Fallback to plain text
            st.warning("⚠️ No SQL returned. Trying fallback text completion...")
            text = complete_text(query)
            if text:
                st.markdown("### ✍️ Fallback Response:")
                st.write(text)
            else:
                st.error("❌ Unable to generate a response.")

# -------------------------
# Run the app
# -------------------------
if __name__ == "__main__":
    main()
