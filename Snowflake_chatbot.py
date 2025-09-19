# Snowflake_chatbot.py (updated)
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
    # Use cache_resource to reuse connection on Streamlit Cloud
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
# Correct generate_sql_from_cortex (uses COMPLETE(VARCHAR, ARRAY, OBJECT) signature)
# -------------------------
def generate_sql_from_cortex(user_query: str):
    """
    Uses SNOWFLAKE.CORTEX.COMPLETE(model, messages_array, options_object)
    - messages_array: ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('role','user','content', ...))
    - options_object: OBJECT_CONSTRUCT('tools', ARRAY_CONSTRUCT( OBJECT_CONSTRUCT(...tool spec...) ), 'temperature', 0 )
    Returns generated SQL string or None.
    """
    if not conn:
        st.error("❌ No active connection.")
        return None

    try:
        # escape single quotes
        safe_query = user_query.replace("'", "''")
        safe_model = SEMANTIC_MODEL_FILE.replace("'", "''")

        # Build the SQL invocation using the ARRAY + OBJECT signature (correct shape)
        cortex_call_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
          'mistral-large',
          ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('role','user','content','{safe_query}')
          ),
          OBJECT_CONSTRUCT(
            'tools', ARRAY_CONSTRUCT(
              OBJECT_CONSTRUCT(
                'type','cortex_analyst_text_to_sql',
                'name','analyst1',
                'semantic_model_file','{safe_model}'
              )
            ),
            'temperature', 0
          )
        ) AS response
        """

        # debug: show generated request SQL in Streamlit logs (not in production)
        st.debug = getattr(st, "debug", None)
        st.write("DEBUG: Executing Cortex COMPLETE call (server side).")
        st.code(cortex_call_sql)

        cur = conn.cursor()
        cur.execute(cortex_call_sql)
        row = cur.fetchone()
        cur.close()

        if not row or not row[0]:
            st.warning("⚠️ Cortex returned empty response.")
            return None

        # row[0] is expected to be a JSON string (object) when using this signature
        try:
            response_obj = json.loads(row[0])
        except Exception:
            # If it's already a dict, accept it
            if isinstance(row[0], dict):
                response_obj = row[0]
            else:
                st.warning("⚠️ Could not parse Cortex response JSON.")
                return None

        # debug: show full response
        st.write("DEBUG: Cortex response object:")
        st.json(response_obj)

        # Look for tool_calls -> arguments -> sql (common place where text-to-sql tool puts SQL)
        tool_calls = response_obj.get("tool_calls") or response_obj.get("tool_calls", [])
        if tool_calls and len(tool_calls) > 0:
            first_tool = tool_calls[0]
            # arguments sometimes embedded as string or as object
            args = first_tool.get("arguments") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {}
            # try common keys where SQL may be present
            sql_text = args.get("sql") or args.get("sql_text") or args.get("query") or None
            if sql_text:
                return sql_text

        # if no tool_calls sql found, attempt to find plain assistant content containing SQL
        choices = response_obj.get("choices") or []
        for c in choices:
            msg = c.get("message") or {}
            content = msg.get("content") or ""
            # naive extraction — look for triple-backtick SQL blocks
            if "```sql" in content or "```" in content:
                # try to extract code fences
                parts = content.split("```")
                for p in parts:
                    if p.lower().strip().startswith("sql") or p.strip().startswith("select ") or p.strip().lower().startswith("with "):
                        # remove optional leading 'sql' label
                        cleaned = p.split("\n",1)[1] if p.lower().startswith("sql") else p
                        return cleaned.strip()
            # fallback: if content contains obvious SQL keywords, return content
            if "select " in content.lower() or "from " in content.lower():
                return content.strip()

        return None

    except Exception as e:
        st.error("❌ Cortex text-to-SQL generation failed: " + str(e))
        # provide traceback in logs for debugging
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
