# Trigger redeploy

import streamlit as st
import json
import pandas as pd
import snowflake.connector

# -------------------------------------------------
# Streamlit config
# -------------------------------------------------
st.set_page_config(page_title="Cortex AI Assistant", layout="wide")
st.title("🤖 Cortex AI Assistant")

# Hide Streamlit's default menu/footer
st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# Load secrets for Snowflake connection
# -------------------------------------------------
try:
    SNOWFLAKE_USER = st.secrets["snowflake"]["USER"]
    SNOWFLAKE_PASSWORD = st.secrets["snowflake"]["PASSWORD"]
    SNOWFLAKE_ACCOUNT = st.secrets["snowflake"]["ACCOUNT"]
    SNOWFLAKE_ROLE = st.secrets["snowflake"]["ROLE"]
    SNOWFLAKE_WAREHOUSE = st.secrets["snowflake"]["WAREHOUSE"]
    SNOWFLAKE_DATABASE = st.secrets["snowflake"]["DATABASE"]
    SNOWFLAKE_SCHEMA = st.secrets["snowflake"]["SCHEMA"]

    # Semantic model on a Snowflake stage (path to the YAML file)
    SEMANTIC_MODEL_FILE = st.secrets["cortex"].get(
        "SEMANTIC_MODEL_FILE",
        '@"GRANTS"."GS"."GSTAGE"/GRANTS_CHATBOT.yaml'
    )

    st.write("DEBUG: Snowflake USER:", SNOWFLAKE_USER)
    st.write("DEBUG: Snowflake ACCOUNT:", SNOWFLAKE_ACCOUNT)
except Exception as e:
    st.error("❌ Missing secrets in .streamlit/secrets.toml")
    st.stop()

# -------------------------------------------------
# Connect to Snowflake using Python Connector
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
# Helper to run a query and return a DataFrame
# -------------------------------------------------
def run_snowflake_query(query):
    if not conn:
        st.error("❌ No active Snowflake connection.")
        return None
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        if not rows:
            return pd.DataFrame(columns=columns)  # return empty DF if no rows
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        st.error(f"❌ SQL Execution Error: {str(e)}")
        return None

# -------------------------------------------------
# Call SNOWFLAKE.CORTEX.COMPLETE to generate SQL
# -------------------------------------------------
def generate_sql_from_cortex(user_query: str) -> str:
    if not conn:
        st.error("❌ No active Snowflake connection.")
        return None
    try:
        # Escape single quotes in user query
        safe_query = user_query.replace("'", "''")
        cortex_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
          'mistral-large',
          OBJECT_CONSTRUCT(
            'messages', ARRAY_CONSTRUCT(
              OBJECT_CONSTRUCT('role','user','content','{safe_query}')
            ),
            'tools', ARRAY_CONSTRUCT(
              OBJECT_CONSTRUCT(
                'type','cortex_analyst_text_to_sql',
                'name','analyst1',
                'parameters', OBJECT_CONSTRUCT(
                    'semantic_model_file','{SEMANTIC_MODEL_FILE}'
                )
              )
            ),
            'temperature', 0
          )
        ) AS response;
        """
        cur = conn.cursor()
        cur.execute(cortex_sql)
        row = cur.fetchone()
        cur.close()

        if row and row[0]:
            response = json.loads(row[0])
            # Expecting tool_calls in the JSON response
            if "tool_calls" in response and len(response["tool_calls"]) > 0:
                tool_call = response["tool_calls"][0]
                args = tool_call.get("arguments", {})
                return args.get("sql", None)
        return None
    except Exception as e:
        st.error(f"❌ Cortex text-to-SQL generation failed: {str(e)}")
        return None

# -------------------------------------------------
# Fallback: simple completion if text-to-SQL fails
# -------------------------------------------------
def complete_text(prompt: str, model="mistral-large") -> str:
    if not conn:
        st.error("❌ No active Snowflake connection.")
        return None
    try:
        safe_prompt = prompt.replace("'", "''")
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{safe_prompt}') AS response;"
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except Exception as e:
        st.error(f"❌ COMPLETE function error: {str(e)}")
        return None

# -------------------------------------------------
# Main Streamlit app
# -------------------------------------------------
def main():
    st.sidebar.header("🔍 Cortex Assistant")
    if "messages" not in st.session_state:
        st.session_state.messages = []

    user_question = st.chat_input("Ask your question...")
    if user_question:
        st.markdown(f"**You asked:** {user_question}")
        with st.spinner("Generating SQL via Cortex... 🤖"):
            sql = generate_sql_from_cortex(user_question)
            if sql:
                results = run_snowflake_query(sql)
                st.markdown("### 🛠️ Generated SQL:")
                st.code(sql, language="sql")
                st.markdown("### 📊 Query Results:")
                if results is not None and not results.empty:
                    st.dataframe(results)
                else:
                    st.warning("⚠️ Query returned no data.")
            else:
                # If no SQL was generated, do a normal text completion
                response_text = complete_text(user_question)
                if response_text:
                    st.markdown("### ✍️ Generated Response:")
                    st.write(response_text)
                else:
                    st.warning("⚠️ Unable to generate a response.")

if __name__ == "__main__":
    main()
