import streamlit as st
import snowflake.connector
import json

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

# ---------------------------
# Connect to Snowflake
# ---------------------------
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )

conn = init_connection()

# ---------------------------
# Initialize Chat History
# ---------------------------
if "messages" not in st.session_state:
    st.session_state["messages"] = [
        {"role": "assistant", "content": "Hello 👋 I’m your Cortex AI Assistant. Ask me anything about your Snowflake data!"}
    ]

# ---------------------------
# Function to Query Cortex Analyst
# ---------------------------
def ask_cortex(question: str):
    try:
        with conn.cursor() as cur:
            query = f"""
            SELECT snowflake.cortex.complete(
              'mistral-large',
              OBJECT_CONSTRUCT(
                'semantic_model', '{SEMANTIC_MODEL}',
                'question', '{question}'
              )
            ) AS response
            """
            cur.execute(query)
            result = cur.fetchone()[0]

            # Parse result safely
            if isinstance(result, str):
                result = json.loads(result)

            # Extract Cortex reply
            reply = result.get("choices", [{}])[0].get("messages", [{}])[0].get("content", "No response")
            return reply

    except Exception as e:
        return f"❌ Error: {e}"

# ---------------------------
# Display Chat Messages
# ---------------------------
for msg in st.session_state["messages"]:
    if msg["role"] == "user":
        with st.chat_message("user"):
            st.markdown(msg["content"])
    else:
        with st.chat_message("assistant"):
            st.markdown(msg["content"])

# ---------------------------
# Chat Input Box
# ---------------------------
if prompt := st.chat_input("Type your question here..."):
    # Add user message
    st.session_state["messages"].append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Query Cortex Analyst
    response = ask_cortex(prompt)

    # Add assistant message
    st.session_state["messages"].append({"role": "assistant", "content": response})
    with st.chat_message("assistant"):
        st.markdown(response)

# ---------------------------
# Sidebar
# ---------------------------
with st.sidebar:
    st.header("⚙️ Settings")
    st.write(f"**User:** {USER}")
    st.write(f"**Role:** {ROLE}")
    st.write(f"**Warehouse:** {WAREHOUSE}")
    st.write(f"**Database.Schema:** {DATABASE}.{SCHEMA}")
    st.divider()
    st.caption("Built with Streamlit + Snowflake Cortex Analyst")
