import streamlit as st
import snowflake.connector
import pandas as pd
import os
from posit import connect

# Page config
st.set_page_config(page_title="Movie Database", page_icon="🎬")

# Title
st.title("🎬 Movie Database")

# Initialize connection automatically
@st.cache_resource
def init_connection():
    # Try to get credentials from environment variables first, then fall back to Streamlit secrets
    if "SNOWFLAKE_ACCOUNT" in os.environ:
        # Use environment variables with OAuth token authentication via Connect SDK
        account = os.environ["SNOWFLAKE_ACCOUNT"]
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "DEFAULT_WH")
        database = os.environ.get("SNOWFLAKE_DATABASE", "DEMOS")
        schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

        # Get user session token
        user_session_token = st.context.headers.get("Posit-Connect-User-Session-Token")

        if not user_session_token:
            st.error("Unable to get user session token. Make sure you're running in Posit Connect.")
            st.stop()

        # Get OAuth access token using Posit Connect SDK
        client = connect.Client()
        credentials = client.oauth.get_credentials(user_session_token)
        access_token = credentials["access_token"]
                
        return snowflake.connector.connect(
            account=account,
            token=access_token,
            authenticator="oauth",
            warehouse=warehouse, 
            database=database,
            schema=schema,
        )
    else:       
        # Fall back to Streamlit secrets with username/password
        account = st.secrets["SNOWFLAKE_ACCOUNT"]
        user = st.secrets.get("SNOWFLAKE_USER")
        password = st.secrets.get("SNOWFLAKE_PASSWORD")
        warehouse = st.secrets.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        database = st.secrets.get("SNOWFLAKE_DATABASE", "YOUR_DATABASE")
        schema = st.secrets.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        
        return snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )   

# Query data
@st.cache_data(ttl=600)
def load_movies():
    conn = init_connection()
    query = "SELECT * FROM JENN_MOVIES ORDER BY rating DESC"
    df = pd.read_sql(query, conn)
    return df
            
# Load the data
try:
    df = load_movies()
    st.session_state['movies_df'] = df
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")
    st.info("Make sure your credentials are configured:\n- Environment variable: SNOWFLAKE_ACCOUNT (uses OAuth via Posit Connect - integration GUID auto-discovered), OR\n- `.streamlit/secrets.toml` file with SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
    st.stop()

# Display movies if data exists
if 'movies_df' in st.session_state:
    df = st.session_state['movies_df']

    # Check if dataframe is empty
    if len(df) == 0:
        st.warning("No movies found in the database!")
        st.info("Make sure you've run the CREATE TABLE and INSERT statements to add movie data to your Snowflake database.")
        st.stop()

    st.subheader(f"Total Movies: {len(df)}")

    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Average Rating", f"{df['RATING'].mean():.2f}")
    with col2:
        st.metric("Highest Rated", df.iloc[0]['TITLE'])
    with col3:
        st.metric("Genres", df['GENRE'].nunique())

    # Display the dataframe
    st.subheader("All Movies")
    st.dataframe(df, use_container_width=True)

    # Filter by genre
    st.subheader("Filter by Genre")
    selected_genre = st.selectbox("Select a genre:", ["All"] + sorted(df['GENRE'].unique().tolist()))

    if selected_genre != "All":
        filtered_df = df[df['GENRE'] == selected_genre]
        st.dataframe(filtered_df, use_container_width=True)

    # Simple chart
    st.subheader("Ratings by Movie")
    chart_data = df.set_index('TITLE')['RATING']
    st.bar_chart(chart_data)

else:
    st.info("👈 Please enter your Snowflake credentials in the sidebar and click Connect")
