import pandas as pd
import plotly.express as px
import streamlit as st
from py2neo import Graph
st.title('Global Trade Statistics')



uri = "bolt://localhost:7687"
username = "neo4j"
pwd = "123456"

try:
    session = Graph(uri, auth=(username, pwd))
    print ('Connected to Neo4j!')
except:
    print ('Could not connect to Neo4j')

# Export and get all data from neo4j
query = '''CALL apoc.export.csv.all("cleaned.csv", {})
'''

@st.cache
def load_data(nrows):
    data = pd.read_csv('cleaned.csv', nrows=nrows)

    return data


main_seletion = st.selectbox('View Visualization',
                             ['Overview by Country',
                              'Major Categories by Country',
                              'Exports/Imports by Country',
                              'Exports/Imports by Country by Weight',
                              'Exports/Imports by Country by Quantity',
                              'Global Trade by Product Category',
                              'Top 10 Countries', 'Top 10 Categories'])

if main_seletion == 'Exports/Imports by Country':
    df = load_data(rows)
    option = st.selectbox('Select Country',
                          df['country_or_area'].unique())
    option2 = st.selectbox('Select Flow',
                           df['flow'].unique())
    df = df.rename(columns={'year': 'index'}).set_index('index')

    new_data = df[(df["country_or_area"] == option) & (df["flow"] == option2)]
    new_data['trade_usd'] = pd.to_numeric(new_data['trade_usd'], errors='coerce')

    st.subheader('%ss for %s over time in US Dollars' % (option2, option))
    st.line_chart(new_data['trade_usd'])


elif main_seletion == 'Major Categories by Country':
    df = load_data(rows)

    option = st.selectbox('Select Country',
                          df['country_or_area'].unique())
    option2 = st.selectbox('Select Flow',
                           df['flow'].unique())

    df2 = df.loc[(df['country_or_area'] == option) & (df['comm_code'] != 'TOTAL') & (df['flow'] == option2)].groupby(
        by=['category'])['trade_usd'].sum()
    df2 = df2.to_frame().reset_index()

    fig3 = px.pie(df2, values=df2.trade_usd, names=df2.category, color=df2.category,
                  )
    fig3.update_layout(
        title="<b>Top %ss for %s</b>" %(option2, option))
    st.plotly_chart(fig3)

    st.dataframe(df2)



elif main_seletion == 'Overview by Country':
    df = load_data(rows)
    option = st.selectbox('Select Country',
                          df['country_or_area'].unique())

    df2 = df.loc[(df['country_or_area'] == option) & (df['comm_code'] != 'TOTAL')].groupby(by=['flow'])[
        'trade_usd'].sum()
    df2 = df2.to_frame().reset_index()

    fig3 = px.pie(df2, values=df2.trade_usd, names=df2.flow, color=df2.flow,
                  )
    fig3.update_layout(
        title="<b>Percentage of Flow for %s</b>" % option)
    st.plotly_chart(fig3)

    st.dataframe(df2)


elif main_seletion == 'Top 10 Countries':
    df = load_data(rows)
    option2 = st.selectbox('Select Flow',
                           df['flow'].unique())

    df2 = df.loc[(df['flow'] == option2) & (df['comm_code'] != 'TOTAL')].groupby(by=['country_or_area'])[
        'trade_usd'].sum()
    df2 = df2.to_frame().reset_index()
    df2 = df2.rename(columns={'country_or_area': 'index'}).set_index('index')
    # st.bar_chart(df2.sort_values('trade_usd',ascending = False).head(10))
    st.bar_chart(df2.sort_values('trade_usd', ascending=False).head(10))
    st.dataframe(df2.sort_values('trade_usd', ascending=False).head(10))

elif main_seletion == 'Top 10 Categories':
    df = load_data(rows)
    option2 = st.selectbox('Select Flow',
                           df['flow'].unique())

    df2 = df.loc[(df['flow'] == option2) & (df['comm_code'] != 'TOTAL')].groupby(by=['category'])[
        'trade_usd'].sum()
    df2 = df2.to_frame().reset_index()
    df2 = df2.rename(columns={'category': 'index'}).set_index('index')
    st.dataframe(df2.sort_values('trade_usd', ascending=False).head(10))
    st.bar_chart(df2.sort_values('trade_usd', ascending=False).head(10))


elif main_seletion == 'Exports/Imports by Country by Weight':
    df = load_data(rows)
    option = st.selectbox('Select Country',
                          df['country_or_area'].unique())
    option2 = st.selectbox('Select Flow',
                           df['flow'].unique())

    df = df.rename(columns={'year': 'index'}).set_index('index')

    new_data = df[(df["country_or_area"] == option) & (df["flow"] == option2)]
    new_data['weight_kg'] = pd.to_numeric(new_data['weight_kg'], errors='coerce')

    st.subheader('%ss for %s over time by total volume in Weight' % (option2, option))
    st.line_chart(new_data['weight_kg'])


elif main_seletion == 'Exports/Imports by Country by Quantity':
    df2 = load_data(rows)
    option = st.selectbox('Select Country',
                          df2['country_or_area'].unique())
    option2 = st.selectbox('Select Flow',
                           df2['flow'].unique())
    df2 = df2.rename(columns={'year': 'index'}).set_index('index')

    new_data = df2[(df2["country_or_area"] == option) & (df2["flow"] == option2)]
    new_data['quantity'] = pd.to_numeric(new_data['quantity'], errors='coerce')

    st.subheader('%ss for %s over time by total number of units' % (option2, option))
    st.line_chart(new_data['quantity'])


elif main_seletion == 'Global Trade by Product Category':
    df = load_data(rows)
    option = st.selectbox('Select Category',
                          df['category'].unique())
    option2 = st.selectbox('Select Flow',
                           df['flow'].unique())

    new_data = df[(df["category"] == option) & (df["flow"] == option2)]

    grouped_multiple = new_data.groupby(['year', 'category', 'flow']).agg({'trade_usd': ['sum']})
    grouped_multiple.columns = ['trade_usd_sum']
    grouped_multiple = grouped_multiple.reset_index()
    grouped_multiple = grouped_multiple.rename(columns={'year': 'index'}).set_index('index')

    # grouped_multiple = grouped_multiple.reset_index()
    print(grouped_multiple)
    st.line_chart(grouped_multiple['trade_usd_sum'])
    # st.bar_chart(new_data['weight_kg'])
    # st.bar_chart(new_data['quantity'])

else:
    st.write('Please make a selection')
