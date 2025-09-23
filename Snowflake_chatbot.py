import streamlit as st
import snowflake.connector
import json
import requests
import pandas as pd
from typing import List, Dict, Any

# ---------------------------
# Page Config
# ---------------------------
st.set_page_config(page_title="Cortex AI Assistant", page_icon="🤖", layout="centered")

# ---------------------------
# Title / Header
# ---------------------------
st.title("🤖 Cortex AI Assistant")
st.caption("Ask natural language questions to your Snowflake data using Cortex Analyst")

# ---------------------------
# Load Secrets from Streamlit
# ---------------------------
USER = st.secrets["snowflake"]["USER"]
PASSWORD = st.secrets["snowflake"]["PASSWORD"]
ACCOUNT = st.secrets["snowflake"]["ACCOUNT"]
ROLE = st.secrets["snowflake"]["ROLE"]
WAREHOUSE = st.secrets["snowflake"]["WAREHOUSE"]
DATABASE = st.secrets["snowflake"]["DATABASE"]
SCHEMA = st.secrets["snowflake"]["SCHEMA"]
SEMANTIC_MODEL = st.secrets["cortex"]["SEMANTIC_MODEL"]
API_TIMEOUT = int(st.secrets["cortex"]["API_TIMEOUT"])  # Not used in requests, but keeping for reference

# ---------------------------
# Connect to Snowflake (ensure your role has SNOWFLAKE.CORTEX_USER granted if needed)
# ---------------------------
@st.cache_resource
def init_connection():
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    # Optional: Grant CORTEX_USER if not already (run once in Snowflake)
    # with conn.cursor() as cur:
    #     cur.execute("USE ROLE ACCOUNTADMIN;")
    #     cur.execute("GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;")
    return conn

conn = init_connection()

# ---------------------------
# Initialize Chat History
# ---------------------------
if "messages" not in st.session_state:
    st.session_state["messages"] = [
        {"role": "assistant", "content": [{"type": "text", "text": "Hello 👋 I’m your Cortex AI Assistant. Ask me anything about your Snowflake data!"}]}
    ]

# ---------------------------
# Function to Query Cortex Analyst via API (send only current question)
# ---------------------------
def ask_cortex(question: str) -> Dict[str, Any]:
    host = f"{ACCOUNT}.snowflakecomputing.com"
    url = f"https://{host}/api/v2/cortex/analyst/message"
    token = conn.rest.token  # Session token from connector for auth

    headers = {
        "Authorization": f"Bearer {token}",  # Standard Bearer format for Cortex API
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": question
                    }
                ]
            }
        ],
        "semantic_model_file": SEMANTIC_MODEL
    }

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        error_details = e.response.text if e.response else str(e)  # Capture full error body
        st.error(f"API Error: {e} - Details: {error_details}")
        return {"error": str(e), "details": error_details}

# ---------------------------
# Function to Execute Generated SQL
# ---------------------------
def execute_sql(sql: str) -> pd.DataFrame:
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(results, columns=columns)
    except Exception as e:
        st.error(f"SQL Execution Error: {e}")
        return pd.DataFrame()

# ---------------------------
# Display Chat Messages
# ---------------------------
for msg in st.session_state["messages"]:
    role = msg["role"]
    content = msg["content"]
    with st.chat_message("user" if role == "user" else "assistant"):
        for part in content:
            if part["type"] == "text":
                st.markdown(part["text"])
            elif part["type"] == "sql":
                st.code(part["statement"], language="sql")
            elif part["type"] == "results":
                st.dataframe(part["df"])

# ---------------------------
# Chat Input Box
# ---------------------------
if prompt := st.chat_input("Type your question here..."):
    # Add user message
    user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}]}
    st.session_state["messages"].append(user_msg)
    with st.chat_message("user"):
        st.markdown(prompt)

    # Query Cortex Analyst with only the current prompt
    api_response = ask_cortex(prompt)

    if "error" not in api_response:
        # Extract analyst response
        analyst_msg = api_response.get("message", {})  # Note: docs use singular "message" in response
        content = analyst_msg.get("content", [])

        # Parse content parts
        summary = next((c["text"] for c in content if c["type"] == "text"), "No summary available.")
        sql = next((c["statement"] for c in content if c["type"] == "sql"), None)

        # Execute SQL if present
        results_df = pd.DataFrame()
        if sql:
            results_df = execute_sql(sql)

        # Build display content
        display_content = []
        if summary:
            display_content.append({"type": "text", "text": f"**Summary of Query Results:**\n{summary}"})
        if sql:
            display_content.append({"type": "sql", "statement": sql})
        if not results_df.empty:
            display_content.append({"type": "results", "df": results_df})
            # Append result to summary for text display
            display_content[0]["text"] += f"\n\n**Query Results ({len(results_df)} rows):**\n{results_df.to_markdown()}"

        # Add to history and display
        assistant_msg = {"role": "assistant", "content": display_content}
        st.session_state["messages"].append(assistant_msg)
        with st.chat_message("assistant"):
            for part in display_content:
                if part["type"] == "text":
                    st.markdown(part["text"])
                elif part["type"] == "sql":
                    st.markdown("**Generated SQL Query:**")
                    st.code(part["statement"], language="sql")
                elif part["type"] == "results":
                    st.markdown("**Query Results:**")
                    st.dataframe(part["df"])
    else:
        error_msg = api_response.get("details", api_response["error"])
        st.session_state["messages"].append({"role": "assistant", "content": [{"type": "text", "text": error_msg}]})
        with st.chat_message("assistant"):
            st.markdown(error_msg)

# ---------------------------
# Sidebar
# ---------------------------
with st.sidebar:
    st.header("⚙️ Settings")
    st.write(f"**User:** {USER}")
    st.write(f"**Role:** {ROLE}")
    st.write(f"**Warehouse:** {WAREHOUSE}")
    st.write(f"**Database.Schema:** {DATABASE}.{SCHEMA}")
    st.write(f"**Semantic Model:** {SEMANTIC_MODEL}")
    st.divider()
    st.caption("Built with Streamlit + Snowflake Cortex Analyst")