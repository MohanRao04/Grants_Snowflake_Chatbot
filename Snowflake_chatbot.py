# Snowflake_chatbot.py (fixed)
import streamlit as st
import json
import pandas as pd
import snowflake.connector
import traceback

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
# Load secrets
# -------------------------
try:
    SNOWFLAKE_USER = st.secrets["snowflake"]["USER"]
    SNOWFLAKE_PASSWORD = st.secrets["snowflake"]["PASSWORD"]
    SNOWFLAKE_ACCOUNT = st.secrets["snowflake"]["ACCOUNT"]
    SNOWFLAKE_ROLE = st.secrets["snowflake"]["ROLE"]
    SNOWFLAKE_WAREHOUSE = st.secrets["snowflake"]["WAREHOUSE"]
    SNOWFLAKE_DATABASE = st.secrets["snowflake"]["DATABASE"]
    SNOWFLAKE_SCHEMA = st.secrets["snowflake"]["SCHEMA"]

    # Use the stage path seen from your LIST output: default '@gstage/GRANTS_CHATBOT.yaml'
    SEMANTIC_MODEL_FILE = st.secrets["cortex"].get("SEMANTIC_MODEL_FILE", "@gstage/GRANTS_CHATBOT.yaml")

    st.write("DEBUG: Snowflake USER:", SNOWFLAKE_USER)
    st.write("DEBUG: Snowflake ACCOUNT:", SNOWFLAKE_ACCOUNT)
    st.write("DEBUG: Semantic model file:", SEMANTIC_MODEL_FILE)
except Exception:
    st.error("❌ Missing secrets in Streamlit Cloud (Settings → Secrets). Ensure [snowflake] and [cortex] blocks exist.")
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
    st.info("✅ Connected to Snowflake using snowflake-connector-python")
except Exception as e:
    st.error(f"❌ Connection failed: {e}")
    conn = None

# -------------------------
# Helpers
# -------------------------
def run_snowflake_query(query: str):
    if not conn:
        st.error("❌ No active connection.")
        return None
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        cur.close()
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"❌ SQL Execution Error: {e}")
        return None

def complete_text(prompt: str, model="mistral-large"):
    """Fallback: plain text completion (not SQL)"""
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
        return row[0] if row else None
    except Exception as e:
        st.error(f"❌ COMPLETE function error: {e}")
        return None

# -------------------------
# Correct generate_sql_from_cortex (uses COMPLETE_ANALYST)
# -------------------------
def generate_sql_from_cortex(user_query: str):
    """
    Uses SNOWFLAKE.CORTEX.COMPLETE_ANALYST with a semantic model.
    Returns generated SQL string or None.
    """
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
        ) AS response
        """

        st.write("DEBUG: Executing Cortex COMPLETE_ANALYST call.")
        st.code(cortex_call_sql, language="sql")

        cur = conn.cursor()
        cur.execute(cortex_call_sql)
        row = cur.fetchone()
        cur.close()

        if not row or not row[0]:
            st.warning("⚠️ Cortex returned empty response.")
            return None

        # COMPLETE_ANALYST returns SQL directly as plain text
        sql_text = row[0].strip()
        return sql_text

    except Exception as e:
        st.error("❌ Cortex text-to-SQL generation failed: " + str(e))
        st.text(traceback.format_exc())
        return None

# -------------------------
# Main app
# -------------------------
def main():
    st.sidebar.header("🔍 Cortex Assistant")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    query = st.chat_input("Ask your question...")

    if not query:
        return

    st.markdown(f"**You asked:** {query}")
    with st.spinner("Generating SQL and results via Cortex..."):
        sql = generate_sql_from_cortex(query)

        if sql:
            st.markdown("### 🛠️ Generated SQL:")
            st.code(sql, language="sql")
            results = run_snowflake_query(sql)
            st.markdown("### 📊 Query Results:")
            if results is not None and not results.empty:
                st.dataframe(results)
            else:
                st.warning("⚠️ No rows returned.")
        else:
            # fallback to plain text completion
            text = complete_text(query)
            if text:
                st.markdown("### ✍️ Generated Response:")
                st.write(text)
            else:
                st.warning("⚠️ Unable to generate SQL or text response from Cortex.")

if __name__ == "__main__":
    main()
