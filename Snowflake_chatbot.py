import streamlit as st
import snowflake.connector
import json
import requests
import pandas as pd

# Streamlit config
st.set_page_config(page_title="Cortex AI Assistant", layout="wide")
st.title("🤖 Cortex AI Assistant")
st.caption("Ask questions to your Snowflake data")

# Hide menu/footer
st.markdown(
    """
    <style>
    #MainMenu, header, footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True
)

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["USER"],
        password=st.secrets["snowflake"]["PASSWORD"],
        account=st.secrets["snowflake"]["ACCOUNT"],
        warehouse=st.secrets["snowflake"]["WAREHOUSE"],
        database=st.secrets["snowflake"]["DATABASE"],
        schema=st.secrets["snowflake"]["SCHEMA"],
        role=st.secrets["snowflake"]["ROLE"]
    )
    st.success("✅ Connected to Snowflake")
except Exception as e:
    st.error(f"❌ Connection failed: {str(e)}")
    conn = None


# Get a valid Snowflake session token for Cortex API
def get_snowflake_token():
    try:
        cur = conn.cursor()
        cur.execute("SELECT SYSTEM$GET_SESSION_TOKEN()")
        token = cur.fetchone()[0]
        cur.close()
        return token
    except Exception as e:
        st.error(f"❌ Failed to get session token: {e}")
        return None


# Call Cortex Analyst
def ask_cortex(question):
    token = get_snowflake_token()
    if not token:
        return None

    account = st.secrets["snowflake"]["ACCOUNT"]
    url = f"https://{account}.snowflakecomputing.com{st.secrets['cortex']['API_ENDPOINT']}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "mistral-large",
        "messages": [{"role": "user", "content": question}],
        "tools": [
            {
                "type": "cortex_analyst_text_to_sql",
                "name": "analyst",
                "semantic_model": st.secrets["cortex"]["SEMANTIC_MODEL"]
            }
        ],
        "temperature": 0.0,
        "max_tokens": 500
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(st.secrets["cortex"]["API_TIMEOUT"]) / 1000)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        st.error(f"❌ API Request Failed: {str(e)}")
        return None


# Process Cortex Analyst response
def process_response(data):
    """Extract SQL + results from Cortex response"""
    sql_query = None
    results = None
    response_text = None

    try:
        choices = data.get("choices", [])
        if not choices:
            return None, None, None

        # Loop over messages
        for choice in choices:
            msgs = choice.get("messages", [])
            for msg in msgs:
                if msg.get("role") == "tool" and "sql" in msg.get("content", {}):
                    sql_query = msg["content"]["sql"]
                elif msg.get("role") == "assistant":
                    response_text = msg.get("content")

        # If we got SQL, run it
        if sql_query:
            cur = conn.cursor()
            cur.execute(sql_query)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            results = pd.DataFrame(rows, columns=cols)
            cur.close()

    except Exception as e:
        st.error(f"❌ Error processing response: {e}")

    return sql_query, results, response_text


# UI
question = st.chat_input("Ask your question...")

if question:
    st.markdown(f"**You asked:** {question}")
    with st.spinner("🤖 Thinking..."):
        data = ask_cortex(question)
        if data:
            sql_query, results, response_text = process_response(data)

            if sql_query:
                st.markdown("### 🛠️ Generated SQL Query")
                st.code(sql_query, language="sql")

            if results is not None and not results.empty:
                st.markdown("### 📊 Query Results")
                st.dataframe(results)

            if response_text:
                st.markdown("### ✍️ Assistant Response")
                st.write(response_text)

        else:
            st.warning("⚠️ No response from Cortex.")
