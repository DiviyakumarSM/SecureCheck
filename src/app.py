import pandas as pd;
import mysql.connector
import streamlit as st
from mysql.connector import Error
from pathlib import Path
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import plotly.express as px


DB_CONFIG = {
    "host": "localhost",
    "user": "john",
    "password": "Indian@001!",
    "database": "dk"
}

def read_xlsx(path):
    return pd.read_excel(str(path))
 
def get_db_connection():
    """Establishes a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("Connection to MySQL DB successful")
        return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

def fetch_all_records(table_name):
    """Fetches all records from a specified table."""
    conn = get_db_connection()
    if conn is None:
        return []

    records = []
    try:
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except Error as e:
        print(f"Error while fetching data: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")
    return records

# --- Function to insert data into a table ---
def insert_record(query, values):
    """Inserts a single record into the database using a parameterized query."""
    conn = get_db_connection()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
        print(f"{cursor.rowcount} record inserted successfully.")
        return True
    except Error as e:
        print(f"Error while inserting data: {e}")
        conn.rollback() # Roll back changes on error
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

# --- Function to update data in a table ---
def update_record(query, values):
    """Updates a record in the database using a parameterized query."""
    conn = get_db_connection()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
        print(f"{cursor.rowcount} record(s) updated successfully.")
        return True
    except Error as e:
        print(f"Error while updating data: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

# --- Function to delete data from a table ---
def delete_record(query, values):
    """Deletes a record from the database using a parameterized query."""
    conn = get_db_connection()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
        print(f"{cursor.rowcount} record(s) deleted successfully.")
        return True
    except Error as e:
        print(f"Error while deleting data: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")
def show_hide_spinner(value):
    if value:
        st.spinner(text="In progress...",show_time=False, width="stretch")
    else:
        st.success()

# def load_dashboard():

def get_donut_chart():
    conn = get_db_connection()
    if conn is None:
        return False
    # show_hide_spinner(True)
    try:
        cursor = conn.cursor()
        cursor.execute("""SELECT COUNTRY_NAME AS "COUNTRY",COUNT(*) AS "COUNT" FROM DK.SECURE_CHECK GROUP BY COUNTRY_NAME;""")
        data = cursor.fetchall()
        # show_hide_spinner(False)
        result = pd.DataFrame(data, columns=['Country', 'Count'])
        return result
    except Error as e:
        print(f"Error while deleting data: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def get_bar_chart():
    conn = get_db_connection()
    if conn is None:
        return False
    # show_hide_spinner(True)
    try:
        cursor = conn.cursor()
        cursor.execute("""SELECT VIOLATION AS "VIOLATION",COUNT(*) AS "COUNT" FROM DK.SECURE_CHECK GROUP BY VIOLATION ORDER BY COUNT DESC;""")
        data = cursor.fetchall()
        # show_hide_spinner(False)
        result = pd.DataFrame(data, columns=['Violation', 'Count'])
        return result
    except Error as e:
        print(f"Error while deleting data: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def get_prediction(stop_date,stop_time,county_name,driver_gender, driver_age, driver_race,search_conducted,search_type,drugs_related_stop,stop_duration,vehicle_number,timestamp):
    query ="""
        
    """
    
    
# --- Example usage ---
if __name__ == "__main__":
    st.set_page_config(layout="wide")
    xlsx_data_df = read_xlsx(Path(__file__).parent.parent / 'data' / 'traffic_stops.xlsx')

   # Create SQLAlchemy engine
    password = quote_plus("Indian@001!")
    engine = create_engine(f"mysql+pymysql://john:{password}@localhost/dk")

    # Write DataFrame to SQL table
    xlsx_data_df.to_sql("secure_check", con=engine, if_exists="replace", index=False)
   
    st.title("Secure Check")
    tab1, tab2, tab3 = st.tabs(["Dashbaord", "New Log & Prediction", "Insights"])
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.header("Stops by Country")
            country_wise_data = get_donut_chart()
            fig = px.pie(
                data_frame=country_wise_data,
                values='Count',
                names='Country',
                title='By Country'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.header("Stops by Country")
            violation_wise_data = get_bar_chart()
            fig = px.bar(
                data_frame=violation_wise_data,
                y='Count',
                x='Violation',
                title='By Violation'
            )
            st.plotly_chart(fig, use_container_width=True)
        # Alternatively, for unequal widths:
        # col1, col2 = st.columns([3, 1]) # col1 will be 3 times wider than col2
        # st.write(xlsx_data_df)
    with tab2:
         st.write("## Enter below details")
         with st.form("new_log_form"):
            stop_date = st.date_input("Stop Date")
            stop_time = st.time_input("Stop Time")
            county_name = st.text_input("County Name")
            driver_gender = st.selectbox("Driver Gender", ["male", "female"])
            driver_age = st.number_input("Driver Age", min_value=16, max_value=100, value=27)
            driver_race = st.text_input("Driver Race")
            search_conducted = st.selectbox("Was a Search Conducted?", ["0", "1"])
            search_type = st.text_input("Search Type")
            drugs_related_stop = st.selectbox("Was it Drug Related?", ["0", "1"])
            stop_duration = st.selectbox("Stop Duration", xlsx_data_df['stop_duration'].dropna().unique())
            vehicle_number = st.text_input("Vehicle Number")
            timestamp = pd. Timestamp.now()
            submitted = st.form_submit_button("Predict Stop Outcome & violation")

            if submitted:
                get_prediction(stop_date,stop_time,county_name,driver_gender, driver_age, driver_race,search_conducted,search_type,drugs_related_stop,stop_duration,vehicle_number,timestamp)
    with tab3:
        st.write("## FAQ")
        queryList = {
            "Top 10 Vehicle involved in drugs ?": """SELECT * FROM (SELECT VEHICLE_NUMBER, SUM(DRUGS_RELATED_STOP) AS "COUNT" FROM DK.SECURE_CHECK GROUP BY VEHICLE_NUMBER) AS DUM ORDER BY COUNT DESC LIMIT 10;""",
            
        }     
    # Example: Insert a new customer
    # print("\n--- Inserting a new customer ---")
    # insert_query = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    # new_customer = ("Michael Scott", "1725 Slough Avenue")
    # insert_record(insert_query, new_customer)

    # Example: Update a customer's address
    # print("\n--- Updating a customer ---")
    # update_query = "UPDATE customers SET address = %s WHERE name = %s"
    # update_values = ("123 Paper Street", "Michael Scott")
    # update_record(update_query, update_values)

    # Example: Delete a customer
    # print("\n--- Deleting a customer ---")
    # delete_query = "DELETE FROM customers WHERE name = %s"
    # delete_values = ("Michael Scott",)
    # delete_record(delete_query, delete_values)

    # Example: Fetch all records again to see changes


    # st.title("Test")

    # print("\n--- Fetching all customers after changes ---")
    # all_customers_after = fetch_all_records("emp_details")
    # if all_customers_after:
    #     for customer in all_customers_after:
    #         print(customer)
    # else:
    #     print("No records found or an error occurred.")

