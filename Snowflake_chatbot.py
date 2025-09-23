import streamlit as st
import snowflake.connector

# ------------------------
# Dummy login credentials
# ------------------------
VALID_USERNAME = "admin"
VALID_PASSWORD = "password123"

# ------------------------
# Streamlit app config
# ------------------------
st.set_page_config(page_title="Snowflake Cortex Chatbot", page_icon="🤖", layout="wide")

# ------------------------
# Session state for login
# ------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# ------------------------
# Login form
# ------------------------
if not st.session_state.logged_in:
    st.title("🔐 Login to Chatbot")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_btn = st.form_submit_button("Login")

    if login_btn:
        if username == VALID_USERNAME and password == VALID_PASSWORD:
            st.session_state.logged_in = True
            st.success("✅ Login successful!")
            st.rerun()
        else:
            st.error("❌ Invalid username or password")

# ------------------------
# Main Chatbot UI
# ------------------------
else:
    st.title("🤖 Cortex AI Assistant")
    st.caption("Ask questions to your Snowflake data")

    # Keep chat history
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # ------------------------
    # Snowflake connection
    # ------------------------
    if "conn" not in st.session_state:
        try:
            conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["USER"],
                password=st.secrets["snowflake"]["PASSWORD"],
                account=st.secrets["snowflake"]["ACCOUNT"],
                role=st.secrets["snowflake"]["ROLE"],
                warehouse=st.secrets["snowflake"]["WAREHOUSE"],
                database=st.secrets["snowflake"]["DATABASE"],
                schema=st.secrets["snowflake"]["SCHEMA"]
            )
            st.session_state.conn = conn
            st.sidebar.success("✅ Connected to Snowflake")
        except Exception as e:
            st.sidebar.error(f"❌ Connection failed: {e}")
            st.stop()

    # -----------------------
    # Chat input
    # -----------------------
    user_msg = st.chat_input("Ask me something about your data...")
    if user_msg:
        st.session_state.chat_history.append({"role": "user", "content": user_msg})

        try:
            # For now, treat input as SQL query
            cur = st.session_state.conn.cursor()
            cur.execute(user_msg)
            rows = cur.fetchall()

            # Convert result into a readable string
            if rows:
                response = "\n".join([str(row) for row in rows])
            else:
                response = "✅ Query executed successfully. No rows returned."

            st.session_state.chat_history.append({"role": "assistant", "content": response})
        except Exception as e:
            error_msg = f"❌ Error executing query: {e}"
            st.session_state.chat_history.append({"role": "assistant", "content": error_msg})

    # ------------------------
    # Display chat history
    # ------------------------
    for chat in st.session_state.chat_history:
        with st.chat_message(chat["role"]):
            st.write(chat["content"])
