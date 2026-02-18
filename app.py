import streamlit as st
from pyspark.sql import SparkSession

st.title("Gold Dashboard")

try:
    spark = SparkSession.builder.getOrCreate()

    df = spark.sql("""
        SELECT attendee_name, duration_minutes
        FROM main.default.Goto_meeting_gold
        LIMIT 1000
    """).toPandas()

    st.success("Data Loaded Successfully")

    st.bar_chart(
        df.groupby("attendee_name")["duration_minutes"].sum()
    )

except Exception as e:
    st.error(f"Error loading data: {e}")
