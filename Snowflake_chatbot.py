# Trigger redeploy

import streamlit as st
import json
import pandas as pd
import requests
import snowflake.connector

# -------------------------------------------------
# Streamlit config
# -------------------------------------------------
st.set_page_config(page_title="Cortex AI Assistant", layout="wide")
st.title("🤖 Cortex AI Assistant")

st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# Load secrets
# -------------------------------------------------
try:
    SNOWFLAKE_USER = st.secrets["snowflake"]["USER"]
    SNOWFLAKE_PASSWORD = st.secrets["snowflake"]["PASSWORD"]
    SNOWFLAKE_ACCOUNT = st.secrets["snowflake"]["ACCOUNT"]
    SNOWFLAKE_ROLE = st.secrets["snowflake"]["ROLE"]
    SNOWFLAKE_WAREHOUSE = st.secrets["snowflake"]["WAREHOUSE"]
    SNOWFLAKE_DATABASE = st.secrets["snowflake"]["DATABASE"]
    SNOWFLAKE_SCHEMA = st.secrets["snowflake"]["SCHEMA"]

    SEMANTIC_MODEL = st.secrets["cortex"].get(
        "SEMANTIC_MODEL",
        '@"GRANTS"."GS"."GSTAGE"/GRANTS_CHATBOT.yaml'
    )

    st.write("DEBUG: Snowflake USER:", SNOWFLAKE_USER)
    st.write("DEBUG: Snowflake ACCOUNT:", SNOWFLAKE_ACCOUNT)
except Exception as e:
    st.error("❌ Missing secrets in .streamlit/secrets.toml")
    st.stop()

# -------------------------------------------------
# Connect to Snowflake (for query execution only)
# -------------------------------------------------
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    st.info("✅ Connected to Snowflake using snowflake-connector-python")
except Exception as e:
    st.error(f"❌ Connection failed: {str(e)}")
    conn = None

# -------------------------------------------------
# Run SQL query in Snowflake
# -------------------------------------------------
def run_snowflake_query(query):
    if not conn:
        st.error("❌ No active connection.")
        return None
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        if not rows:
            return None
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        st.error(f"❌ SQL Execution Error: {str(e)}")
        return None

# -------------------------------------------------
# Cortex REST API call (replaces ANALYZE)
# -------------------------------------------------
def generate_sql_from_cortex(user_query):
    if not conn:
        st.error("❌ No active connection.")
        return None
    try:
        cur = conn.cursor()
        cortex_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            PARSE_JSON($$
              [{{"role":"user","content":"{user_query}"}}]
            $$),
            OBJECT_CONSTRUCT(
              'tools', ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                  'type','cortex_analyst_text_to_sql',
                  'semantic_model','{SEMANTIC_MODEL}'
                )
              )
            )
        ) AS response
        """
        cur.execute(cortex_sql)
        row = cur.fetchone()
        cur.close()

        if row and row[0]:
            import json
            response = json.loads(row[0])

            if "tool_calls" in response and len(response["tool_calls"]) > 0:
                return response["tool_calls"][0].get("sql_text")
        return None
    except Exception as e:
        st.error(f"❌ Cortex Analyst SQL generation failed: {str(e)}")
        return None

# -------------------------------------------------
# COMPLETE fallback
# -------------------------------------------------
def complete(prompt, model="mistral-large"):
    try:
        cur = conn.cursor()
        prompt_escaped = prompt.replace("'", "''")
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt_escaped}') AS response"
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        if result:
            return result[0]
        return None
    except Exception as e:
        st.error(f"❌ COMPLETE Function Error: {str(e)}")
        return None

# -------------------------------------------------
# Main app
# -------------------------------------------------
def main():
    st.sidebar.header("🔍 Cortex Assistant")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    query = st.chat_input("Ask your question...")

    if query:
        st.markdown(f"**You asked:** {query}")
        with st.spinner("Fetching data... 🤖"):
            sql = generate_sql_from_cortex(query)

            if sql:
                results = run_snowflake_query(sql)
                st.markdown("### 🛠️ Generated SQL Query:")
                st.code(sql, language="sql")
                st.markdown("### 📊 Summary of Query Results:")
                st.write("The SQL query retrieves data based on the input question.")
                if results is not None and not results.empty:
                    st.markdown("### 📈 Query Results:")
                    st.dataframe(results)
                else:
                    st.warning("⚠️ No data found.")
            else:
                response_text = complete(query)
                if response_text:
                    st.markdown("### ✍️ Generated Response:")
                    st.write(response_text)
                else:
                    st.warning("⚠️ Unable to generate a response.")

if __name__ == "__main__":
    main()
