import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Gold Dashboard", layout="wide")

st.title("Gold Layer Dashboard")

spark = SparkSession.builder.getOrCreate()

# IMPORTANT: use full table name
df = spark.table("main.default.Goto_meeting_gold").toPandas()

st.metric("Total Records", len(df))

st.subheader("Duration by Attendee")
st.bar_chart(
    df.groupby("attendee_name")["duration_minutes"].sum()
)
