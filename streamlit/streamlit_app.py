import streamlit as st
from streamlit.components.v1 import html
import pandas as pd
from datetime import datetime
import mysql.connector

mydb = mysql.connector.connect(
  host="XXX",
  port=XXX,
  user="XXX",
  password="XXX",
  database="XXX"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT valor, data_atualializacao FROM cotacao order by id desc")

valores = []

datas = []

for (valor, data_atualializacao) in mycursor:
  valores.append(float(valor))
  datas.append(data_atualializacao)
  

for i in datas:
    print(i)
    
for i in valores:
    print(i)

st.set_page_config(layout="wide")

st.header("Cotação Bitcoin")  

df = pd.DataFrame({
  'data': datas,
  'valores': valores
})


df = df.rename(columns={'data':'index'}).set_index('index')

st.line_chart(df)

#cli: streamlit run .\streamlit_app.py