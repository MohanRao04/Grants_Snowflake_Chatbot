# Trigger redeploy

import streamlit as st
import json
import os
import requests
import pandas as pd
import snowflake.connector

# Streamlit config
st.set_page_config(page_title="Cortex AI Assistant", layout="wide")
st.title("🤖 Cortex AI Assistant")

st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --------------------------
# Load secrets
# --------------------------
try:
    SNOWFLAKE_USER = st.secrets["snowflake"]["USER"]
    SNOWFLAKE_PASSWORD = st.secrets["snowflake"]["PASSWORD"]
    SNOWFLAKE_ACCOUNT = st.secrets["snowflake"]["ACCOUNT"]
    SNOWFLAKE_ROLE = st.secrets["snowflake"]["ROLE"]
    SNOWFLAKE_WAREHOUSE = st.secrets["snowflake"]["WAREHOUSE"]
    SNOWFLAKE_DATABASE = st.secrets["snowflake"]["DATABASE"]
    SNOWFLAKE_SCHEMA = st.secrets["snowflake"]["SCHEMA"]

    API_ENDPOINT = st.secrets["cortex"]["API_ENDPOINT"]
    API_TIMEOUT = int(st.secrets["cortex"].get("API_TIMEOUT", 50000))
    SEMANTIC_MODEL = st.secrets["cortex"].get(
        "SEMANTIC_MODEL",
        '@"GRANTS"."GS"."GSTAGE"/GRANTS_CHATBOT.yaml'
    )

    st.write("DEBUG: Snowflake USER:", SNOWFLAKE_USER)
    st.write("DEBUG: Snowflake ACCOUNT:", SNOWFLAKE_ACCOUNT)
except Exception as e:
    st.error("❌ Missing secrets in .streamlit/secrets.toml")
    st.stop()

# --------------------------
# Connect to Snowflake
# --------------------------
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

# --------------------------
# Run SQL query
# --------------------------
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

# --------------------------
# COMPLETE fallback
# --------------------------
def complete(prompt, model="mistral-large"):
    if not conn:
        st.error("❌ No active connection.")
        return None
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

# --------------------------
# Cortex API Call
# --------------------------
def snowflake_api_call(query, semantic_model=SEMANTIC_MODEL):
    if not conn:
        st.error("❌ No active connection.")
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_SESSION()")
        session_id = cur.fetchone()[0]
        cur.close()

        st.info(f"✅ Session ID: {session_id}")

        # Build Cortex API URL correctly
        url = f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com{API_ENDPOINT}"
        headers = {
            "Authorization": f"Bearer {SNOWFLAKE_PASSWORD}",  # ⚠️ Replace with correct auth
            "Content-Type": "application/json"
        }
        payload = {
            "model": "mistral-large",
            "temperature": 0.0,
            "messages": [{"role": "user", "content": query}],
            "tools": [
                {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "analyst1",
                    "semantic_model": semantic_model
                }
            ],
            "max_tokens": 500,
            "stream": True
        }

        st.write("Request URL:", url)
        st.json(payload)

        response = requests.post(url, headers=headers, json=payload, timeout=API_TIMEOUT / 1000)
        response.raise_for_status()

        events = []
        for line in response.iter_lines():
            if line:
                line = line.decode("utf-8").strip()
                if line.startswith("data:"):
                    data = json.loads(line[5:])
                    events.append(data)
        return events
    except Exception as e:
        st.error(f"❌ API Request Failed: {str(e)}")
        return None

# --------------------------
# Process Cortex response
# --------------------------
def process_sse_response(response):
    sql = ""
    if not response:
        return sql
    try:
        for event in response:
            if isinstance(event, dict) and event.get("event") == "message.delta":
                data = event.get("data", {})
                delta = data.get("delta", {})
                for content_item in delta.get("content", []):
                    if content_item.get("type") == "tool_results":
                        tool_results = content_item.get("tool_results", {})
                        if "content" in tool_results:
                            for result in tool_results["content"]:
                                if result.get("type") == "json":
                                    result_data = result.get("json", {})
                                    if "sql" in result_data:
                                        sql += " " + result_data.get("sql", "")
    except Exception as e:
        st.error(f"❌ Error Processing Response: {str(e)}")
    return sql.strip()

# --------------------------
# Main app
# --------------------------
def main():
    st.sidebar.header("🔍 Cortex Assistant")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    query = st.chat_input("Ask your question...")

    if query:
        st.markdown(f"**You asked:** {query}")
        with st.spinner("Fetching data... 🤖"):
            response = snowflake_api_call(query)
            sql = process_sse_response(response)

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
