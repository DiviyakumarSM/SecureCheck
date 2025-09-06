import pandas as pd
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

def get_donut_chart():
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""SELECT COUNTRY_NAME AS "COUNTRY",COUNT(*) AS "COUNT" FROM DK.SECURE_CHECK GROUP BY COUNTRY_NAME;""")
        data = cursor.fetchall()
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
    try:
        cursor = conn.cursor()
        cursor.execute("""SELECT VIOLATION AS "VIOLATION",COUNT(*) AS "COUNT" FROM DK.SECURE_CHECK GROUP BY VIOLATION ORDER BY COUNT DESC;""")
        data = cursor.fetchall()
        result = pd.DataFrame(data, columns=['Violation', 'Count'])
        return result
    except Error as e:
        print(f"Error while deleting data: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def show_query_result(query):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]
        result = pd.DataFrame(data,columns=column_names)
        return result
    except Error as e:
        print(f"Error while deleting data: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def clean_up():
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE secure_check SET driver_age = driver_age_raw WHERE driver_age_raw BETWEEN 15 AND 90;
            UPDATE secure_check SET driver_age = (SELECT * FROM (SELECT AVG(driver_age_raw) FROM secure_check WHERE driver_age_raw IS NOT NULL) AS t) WHERE driver_age IS NULL OR driver_age < 15 OR driver_age > 90;
            ALTER TABLE secure_check DROP COLUMN driver_age_raw;
        """,)
        conn.commit()
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
    
if __name__ == "__main__":
    st.set_page_config(layout="wide")
    xlsx_data_df = read_xlsx(Path(__file__).parent.parent / 'data' / 'traffic_stops.xlsx')

    password = quote_plus("Indian@001!")
    engine = create_engine(f"mysql+pymysql://john:{password}@localhost/dk")

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
            st.header("Stops by Violation")
            violation_wise_data = get_bar_chart()
            fig = px.bar(
                data_frame=violation_wise_data,
                y='Count',
                x='Violation',
                title='By Violation'
            )
            st.plotly_chart(fig, use_container_width=True)
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
            timestamp = pd.Timestamp.now()
            submitted = st.form_submit_button("Predict Stop Outcome & violation")

            if submitted:
                filtered_data = xlsx_data_df[
                    (xlsx_data_df['driver_gender'] == driver_gender) &
                    (xlsx_data_df['driver_age' ] == driver_age) &
                    (xlsx_data_df['search_conducted'] == int(search_conducted)) &
                    (xlsx_data_df['stop_duration'] == stop_duration) &
                    (xlsx_data_df['drugs_related_stop'] == int(drugs_related_stop))
                ]
                if not filtered_data.empty:
                    predicted_outcome = filtered_data['stop_outcome'].mode()[0]
                    predicted_violation = filtered_data['violation'].mode()[0]
                else:
                    predicted_outcome = "warning"
                    predicted_violation = "speeding"

                search_text = "A search was conducted" if int(search_conducted) else "No search was conducted"
                drug_text = "was drug-related" if int(drugs_related_stop) else "was not drug-related"

                st.markdown(f"""
                    ** Prediction Summary **

                    - ** Predicted Violation :** {predicted_violation}
                    - ** Predicted Stop Outcome :** {predicted_outcome}

                    A {driver_age}-year-old {driver_gender} driver in {county_name} was stopped at {stop_time.strftime('%I:%M %p')} on {stop_date}
                    {search_text}, and the stop {drug_text}.
                    Stop duration: ** {stop_duration} **.
                    Vehicle Number: ** {vehicle_number} **.""")
    with tab3:
        col1,col2 = st.columns(2)
        with col1:
            st.write("### Medium")
            queryList = {
                "Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)": """SELECT stop_year, country_name, total_stops, total_arrests, (total_arrests * 100.0 / total_stops) AS arrest_percentage_yearly, SUM(total_stops) OVER (PARTITION BY stop_year) AS total_stops_in_year, SUM(total_arrests) OVER (PARTITION BY stop_year) AS total_arrests_in_year FROM ( SELECT YEAR(stop_datetime) AS stop_year, country_name, COUNT(*) AS total_stops, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests FROM dk.secure_check WHERE stop_datetime IS NOT NULL AND country_name IS NOT NULL GROUP BY stop_year, country_name ) AS yearly_country_stats ORDER BY stop_year, country_name;""",
                "Driver Violation Trends Based on Age and Race (Join with Subquery)": """SELECT violation_trends.age_group, violation_trends.driver_race, violation_trends.violation, violation_trends.violation_count, (violation_trends.violation_count * 100.0 / total_violations.total_count) AS percentage_of_all_violations_for_group FROM ( SELECT CASE WHEN driver_age < 18 THEN 'Under 18' WHEN driver_age BETWEEN 18 AND 24 THEN '18-24' WHEN driver_age BETWEEN 25 AND 34 THEN '25-34' WHEN driver_age BETWEEN 35 AND 44 THEN '35-44' WHEN driver_age BETWEEN 45 AND 54 THEN '45-54' WHEN driver_age >= 55 THEN '55+' ELSE 'Unknown' END AS age_group, driver_race, violation, COUNT(*) AS violation_count FROM dk.secure_check WHERE driver_age IS NOT NULL AND driver_race IS NOT NULL AND violation IS NOT NULL GROUP BY age_group, driver_race, violation ) AS violation_trends INNER JOIN ( SELECT CASE WHEN driver_age < 18 THEN 'Under 18' WHEN driver_age BETWEEN 18 AND 24 THEN '18-24' WHEN driver_age BETWEEN 25 AND 34 THEN '25-34' WHEN driver_age BETWEEN 35 AND 44 THEN '35-44' WHEN driver_age BETWEEN 45 AND 54 THEN '45-54' WHEN driver_age >= 55 THEN '55+' ELSE 'Unknown' END AS age_group, driver_race, COUNT(*) AS total_count FROM dk.secure_check WHERE driver_age IS NOT NULL AND driver_race IS NOT NULL AND violation IS NOT NULL GROUP BY age_group, driver_race ) AS total_violations ON violation_trends.age_group = total_violations.age_group AND violation_trends.driver_race = total_violations.driver_race ORDER BY violation_trends.age_group, violation_trends.driver_race, percentage_of_all_violations_for_group DESC;"""
            }
            selected_key = st.selectbox(
                'Select a query:',
                key="medium",
                options=list(queryList.keys()),
            )
            if selected_key:
                selected_value = queryList[selected_key]
                st.write(show_query_result(selected_value))

        with col2:
            st.write("### Complex")
            queryList2 = {
                "Top 10 Vehicle involved in drugs?": """SELECT * FROM (SELECT VEHICLE_NUMBER, SUM(DRUGS_RELATED_STOP) AS "COUNT" FROM DK.SECURE_CHECK GROUP BY VEHICLE_NUMBER) AS DUM ORDER BY COUNT DESC LIMIT 10;""",
                "Which vehicles were most frequently searched?": """SELECT vehicle_Number, COUNT(*) AS search_count FROM dk.secure_check WHERE search_conducted = TRUE GROUP BY vehicle_Number ORDER BY search_count DESC LIMIT 10;""",
                "Which driver age group had the highest arrest rate?": """SELECT CASE WHEN driver_age < 18 THEN 'Under 18' WHEN driver_age BETWEEN 18 AND 24 THEN '18-24' WHEN driver_age BETWEEN 25 AND 34 THEN '25-34' WHEN driver_age BETWEEN 35 AND 44 THEN '35-44' WHEN driver_age BETWEEN 45 AND 54 THEN '45-54' WHEN driver_age >= 55 THEN '55+' ELSE 'Unknown' END AS age_group, COUNT(CASE WHEN is_arrested = TRUE THEN 1 END) AS total_arrests, COUNT(*) AS total_drivers_in_group, (COUNT(CASE WHEN is_arrested = TRUE THEN 1 END) * 100.0 / COUNT(*)) AS arrest_rate_percentage FROM dk.secure_check WHERE driver_age IS NOT NULL AND driver_age >= 15 GROUP BY age_group ORDER BY arrest_rate_percentage DESC;""",
                "What is the gender distribution of drivers stopped in each country?": """SELECT country_name, driver_gender, COUNT(*) AS number_of_stops FROM dk.secure_check GROUP BY country_name, driver_gender ORDER BY country_name, number_of_stops DESC;""",
                "Which race and gender combination has the highest search rate?":"""SELECT driver_race, driver_gender, COUNT(CASE WHEN search_conducted = TRUE THEN 1 END) AS total_searches, COUNT(*) AS total_stops, (COUNT(CASE WHEN search_conducted = TRUE THEN 1 END) * 100.0 / COUNT(*)) AS search_rate_percentage FROM dk.secure_check WHERE driver_race IS NOT NULL AND driver_gender IS NOT NULL GROUP BY driver_race, driver_gender ORDER BY search_rate_percentage DESC;""",
                "What time of day sees the most traffic stops?":"""SELECT HOUR(stop_time) AS hour_of_day, COUNT(*) AS number_of_stops FROM dk.secure_check GROUP BY hour_of_day ORDER BY number_of_stops DESC LIMIT 1;""",
                "What is the average stop duration for different violations?":"""SELECT violation, AVG( CASE WHEN stop_duration = '<5 min' THEN 2.5 WHEN stop_duration = '6-15 min' THEN 10.5 WHEN stop_duration = '16-30 min' THEN 23 WHEN stop_duration = '30+ min' THEN 35 ELSE NULL END ) AS average_duration_minutes FROM dk.secure_check WHERE violation IS NOT NULL GROUP BY violation ORDER BY average_duration_minutes DESC;""",
                "Are stops during the night more likely to lead to arrests?":"""SELECT CASE WHEN HOUR(stop_time) >= 19 OR HOUR(stop_time) < 6 THEN 'Night' ELSE 'Day' END AS time_of_day, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests, COUNT(*) AS total_stops, (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS arrest_rate_percentage FROM dk.secure_check WHERE stop_time IS NOT NULL GROUP BY time_of_day;""",
            }
            selected_key = st.selectbox(
                'Select a query:',
                key="complex",
                options=list(queryList2.keys()),
            )
            if selected_key:
                selected_value = queryList2[selected_key]
                st.write(show_query_result(selected_value))