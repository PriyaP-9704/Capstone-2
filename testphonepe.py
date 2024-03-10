# import Packages
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pandas as pd
import plotly.express as px
import requests
import json
import os

#sql connection

config = {'user':'root',
          'password':'1234',
          'host':'127.0.0.1',
          'database':'phonepedata'}


connection = mysql.connector.connect(**config)
cursor = connection.cursor(buffered=True)

# Dataframe creation = Agg_transaction

cursor.execute("SELECT * FROM aggregated_transaction")
connection.commit()
table1 = cursor.fetchall()

Agg_transaction = pd.DataFrame(table1,columns=('States', 'Years', 'Quarter', 'Transaction_type', 'Transaction_count',
       'Transaction_amount'))


# Dataframe creation = Aggregated user

cursor.execute("SELECT * FROM aggregated_user")
connection.commit()
table2 = cursor.fetchall()

Agg_user = pd.DataFrame(table2,columns=('States', 'Years', 'Quarter', 'Brand', 'Transaction_count',
       'Percentage'))

# Dataframe creation = map_transaction

cursor.execute("SELECT * FROM map_transaction")
connection.commit()
table3 = cursor.fetchall()

Map_transaction = pd.DataFrame(table3,columns=('States', 'Years', 'Quarter', 'Districts', 'Transaction_count',
       'Transaction_amount'))

# Dataframe creation = map_user

cursor.execute("SELECT * FROM map_user")
connection.commit()
table4 = cursor.fetchall()

Map_user = pd.DataFrame(table4,columns=('States', 'Years', 'Quarter', 'Districts', 'RegisteredUsers',
       'AppOpens'))

# Dataframe creation = top_transaction_districtwise

cursor.execute("SELECT * FROM top_transaction_districtwise")
connection.commit()
table5 = cursor.fetchall()

Top_transaction_districtwise = pd.DataFrame(table5,columns=('States', 'Years', 'Quarter', 'Districts', 'Transaction_count',
       'Transaction_amount'))

# Dataframe creation = top_transaction_pincodewise

cursor.execute("SELECT * FROM top_transaction_pincodewise")
connection.commit()
table6 = cursor.fetchall()

Top_transaction_pincodewise = pd.DataFrame(table6,columns=('States', 'Years', 'Quarter', 'Pincode', 'Transaction_count',
       'Transaction_amount'))

# Dataframe creation = top_user_districtwise

cursor.execute("SELECT * FROM top_user_districtwise")
connection.commit()
table7 = cursor.fetchall()

Top_user_districtwise = pd.DataFrame(table7,columns=('States', 'Years', 'Quarter', 'District', 'RegisteredUsers'))


# Dataframe creation = top_user_pincodewise

cursor.execute("SELECT * FROM top_user_pincodewise")
connection.commit()
table8 = cursor.fetchall()

Top_user_pincodewise = pd.DataFrame(table8,columns=('States', 'Years', 'Quarter', 'Pincode', 'RegisteredUsers'))


#year wise data function:

def Transaction_year(year,df):
    Tran_amo_cou_year = df[df["Years"]== year]
    Tran_amo_cou_year.reset_index(drop = True, inplace= True)

    return Tran_amo_cou_year

# Transaction_amount_count_yearwise --- Analysis

def Transaction_amount_count_yearwise(year,df):
    Tran_amo_cou_year = df[df["Years"]== year]
    Tran_amo_cou_year.reset_index(drop = True, inplace= True)

    Tran_amo_cou_year_group = Tran_amo_cou_year.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    Tran_amo_cou_year_group.reset_index(inplace = True)

    col1,col2 = st.columns(2)
    with col1:
        
        fig_amount = px.bar(Tran_amo_cou_year_group, x ="States", y = "Transaction_amount", title = f"{year} TRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Aggrnyl, height= 650, width= 600)

        st.plotly_chart(fig_amount)
    
    with col2:

        fig_count = px.bar(Tran_amo_cou_year_group, x ="States", y = "Transaction_count", title = f"{year} TRANSACTION COUNT",
                            color_discrete_sequence= px.colors.sequential.Bluered,height= 650, width= 600)
        
        st.plotly_chart(fig_count)
    
    col1, col2 = st.columns(2)

    with col1:

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)

        state_name = []

        data = json.loads(response.content)
        for feature in data["features"]:
            state_name.append(feature["properties"]["ST_NM"])

        state_name.sort()

        fig_india = px.choropleth(Tran_amo_cou_year_group, geojson = data, locations= "States",
                                featureidkey= "properties.ST_NM", color = "Transaction_amount", color_continuous_scale= "Rainbow",
                                range_color=(Tran_amo_cou_year_group["Transaction_amount"].min(),
                                Tran_amo_cou_year_group["Transaction_amount"].max()),
                                hover_name = "States",title= f"{year} Transaction Amount",fitbounds= "locations",
                                height= 650, width= 600)
        fig_india.update_geos(visible = False)

        st.plotly_chart(fig_india)

    with col2:

        fig_india_2 = px.choropleth(Tran_amo_cou_year_group, geojson = data, locations= "States",
                                featureidkey= "properties.ST_NM", color = "Transaction_count", color_continuous_scale= "Rainbow",
                                range_color=(Tran_amo_cou_year_group["Transaction_count"].min(),
                                Tran_amo_cou_year_group["Transaction_count"].max()),
                                hover_name = "States",title= f"{year} Transaction Count",fitbounds= "locations",
                                height= 650, width= 600)
        fig_india_2.update_geos(visible = False)

        st.plotly_chart(fig_india_2)

# Transaction Quarter data:

def Transaction_quarter_data(quarter,df):
   
    Tran_amo_cou_year = df[df["Quarter"]== quarter]
    Tran_amo_cou_year.reset_index(drop = True, inplace= True) 

    return Tran_amo_cou_year
   
# Transaction_amount_count_ year and Quarter wise  --- Analysis

def Transaction_amount_count_quarter(quarter,df):
   
    #Tran_amo_cou_year = df[df["Quarter"]== quarter]
    #Tran_amo_cou_year.reset_index(drop = True, inplace= True)

    Tran_amo_cou_year_group = df.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    Tran_amo_cou_year_group.reset_index(inplace = True)
    col1, col2 = st.columns(2)
    with col1:

        fig_amount = px.bar(Tran_amo_cou_year_group, x ="States", y = "Transaction_amount", title = f"{df["Years"].min()} {quarter}Q TRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Aggrnyl)

        st.plotly_chart(fig_amount)
    with col2:

        fig_count = px.bar(Tran_amo_cou_year_group, x ="States", y = "Transaction_count", title = f"{df["Years"].min()} {quarter}Q TRANSACTION COUNT",
                            color_discrete_sequence= px.colors.sequential.Bluered)

        st.plotly_chart(fig_count)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)

    state_name = []

    data = json.loads(response.content)
    for feature in data["features"]:
        state_name.append(feature["properties"]["ST_NM"])

    state_name.sort()
    
    col1,col2 = st.columns(2)

    with col1:
        fig_india = px.choropleth(Tran_amo_cou_year_group, geojson = data, locations= "States",
                                featureidkey= "properties.ST_NM", color = "Transaction_amount", color_continuous_scale= "Rainbow",
                                range_color=(Tran_amo_cou_year_group["Transaction_amount"].min(),
                                Tran_amo_cou_year_group["Transaction_amount"].max()),
                                hover_name = "States",title= f"{df["Years"].min()} {quarter}Q Transaction Amount",fitbounds= "locations",
                                height= 650, width= 600)
        fig_india.update_geos(visible = False)

        st.plotly_chart(fig_india)

    with col2:
        fig_india_2 = px.choropleth(Tran_amo_cou_year_group, geojson = data, locations= "States",
                                featureidkey= "properties.ST_NM", color = "Transaction_count", color_continuous_scale= "Rainbow",
                                range_color=(Tran_amo_cou_year_group["Transaction_count"].min(),
                                Tran_amo_cou_year_group["Transaction_count"].max()),
                                hover_name = "States",title= f"{df["Years"].min()} {quarter}Q Transaction Count",fitbounds= "locations",
                                height= 650, width= 600)
        fig_india_2.update_geos(visible = False)

        st.plotly_chart(fig_india_2)

# Agg_Transaction Statewise pie plot - year data

def Aggre_Trans_Type_state_year(df,state):

    Tran_amo_cou_year_group = df.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    Tran_amo_cou_year_group.reset_index(inplace = True)

    col1, col2 = st.columns(2)
    with col1:
        fig_pie = px.pie(data_frame= Tran_amo_cou_year_group, names= "Transaction_type",
                        values= "Transaction_amount",width= 600, title= f"{state.upper()} Transaction Amount {df["Years"].min()} ",
                        hole= 0.5)

        st.plotly_chart(fig_pie)

    with col2:    
        fig_pie_2 = px.pie(data_frame= Tran_amo_cou_year_group, names= "Transaction_type",
                        values= "Transaction_count",width= 600, title= f"{state.upper()} Transaction Count {df["Years"].min()}",
                        hole= 0.5)

        st.plotly_chart(fig_pie_2)

# Agg_Transaction Statewise pie plot - Quarter data

def Aggre_Trans_Type_state_quarter(df,state):

    Tran_amo_cou_year_group = df.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    Tran_amo_cou_year_group.reset_index(inplace = True)

    col1,col2 = st.columns(2)
    with col1:
        fig_pie = px.pie(data_frame= Tran_amo_cou_year_group, names= "Transaction_type",
                        values= "Transaction_amount",width= 600, title= f"{state.upper()} Transaction Amount {df["Years"].min()} {df["Quarter"].min()}Q",
                        hole= 0.5)

        st.plotly_chart(fig_pie)

    with col2:   
        fig_pie_2 = px.pie(data_frame= Tran_amo_cou_year_group, names= "Transaction_type",
                        values= "Transaction_count",width= 600, title= f"{state.upper()} Transaction Count {df["Years"].min()} {df["Quarter"].min()}Q",
                        hole= 0.5)

        st.plotly_chart(fig_pie_2)
    
# Aggregated User Analysis:

def Aggre_user_plot_year(df,year):
    
    #-----groupby brands

    Agg_user_groupby = pd.DataFrame(df.groupby("Brand")["Transaction_count"].sum())

    Agg_user_groupby.reset_index(inplace= True)

    fig_bar1 = px.bar(Agg_user_groupby, x= "Brand", y= "Transaction_count",
                    title= f"{year} Brands and Transaction Count", width= 1000,
                    color_discrete_sequence= px.colors.sequential.Bluered_r,
                    hover_name= "Brand")

    st.plotly_chart(fig_bar1)

# Aggregated user Quarter wise Analysis
    

def Aggre_user_plot_quarter(df,year,quarter):


    Agg_user_groupby = pd.DataFrame(df.groupby("Brand")["Transaction_count"].sum())

    Agg_user_groupby.reset_index(inplace= True)

    fig_bar1 = px.bar(Agg_user_groupby, x= "Brand", y= "Transaction_count",
                    title= f"{year} {quarter}Q Brands and Transaction Count", width= 1000,
                    color_discrete_sequence= px.colors.sequential.Bluered_r,
                    hover_name= "Brand")

    st.plotly_chart(fig_bar1)

# Aggre user state plot - Quarter wise

def Aggre_user_plot_state_Q(df,year,state):

    Auqsg = df[df["States"] == state]

    Auqsg.reset_index(drop= True, inplace= True)

    fig_line1 = px.line(Auqsg,x= "Brand",y= "Transaction_count", hover_data= "Percentage",
                        title= f"{state} {year} {Auqsg["Quarter"].min()}Q Brand,Transaction_count,Percentage",
                        markers= True)
    st.plotly_chart(fig_line1)

# Aggre user state plot - Year wise

def Aggre_user_plot_state_Y(df,year,state):

    Auqsg = df[df["States"] == state]

    Auqsg.reset_index(drop= True, inplace= True)

    fig_line2 = px.line(Auqsg,x= "Brand",y= "Transaction_count", hover_data= "Percentage",
                        title= f"{state} {year} Brand,Transaction_count,Percentage",
                        markers= True)
    st.plotly_chart(fig_line2)

# Map Transaction states based District

def Map_Transaction_district(df,state):

    Tran_amo_cou_year = df[df["States"]== state]
    Tran_amo_cou_year.reset_index(drop = True, inplace= True)

    Tran_amo_cou_year_group = Tran_amo_cou_year.groupby("Districts")[["Transaction_count","Transaction_amount"]].sum()
    Tran_amo_cou_year_group.reset_index(inplace = True)

    col1, col2 = st.columns(2)
    with col1:
        fig_pie = px.bar(Tran_amo_cou_year_group, x= "Transaction_amount", y= "Districts", orientation= "h",
                        height= 600,title= f"{state} {Tran_amo_cou_year["Years"].min()} Districts and Transaction Amount",
                        color_discrete_sequence= px.colors.sequential.Mint_r)

        st.plotly_chart(fig_pie)

    with col2:
        fig_pie_2 = px.bar(Tran_amo_cou_year_group, x= "Transaction_count", y= "Districts", orientation= "h",
                     height= 600,title= f"{state} {Tran_amo_cou_year["Years"].min()} Districts and Transaction Count",
                     color_discrete_sequence= px.colors.sequential.Mint_r)

        st.plotly_chart(fig_pie_2)

# Map user Analysis state - yearwise
    
def Map_user_plot_state(df, year):

    Map_user_groupby = df.groupby("States")[["RegisteredUsers","AppOpens"]].sum()
    Map_user_groupby.reset_index(inplace= True)

    fig_line1 = px.line(Map_user_groupby,x= "States",y= ["RegisteredUsers","AppOpens"], hover_data= "States",
                            title= f"{year} RegisterUsers, AppOpens", width= 1000,
                            markers= True)
    st.plotly_chart(fig_line1)


# Map user Analysis state - Quarter wise
    
def Map_user_plot_state_Q(df, year,quarter):

    Map_user_groupby = df.groupby("States")[["RegisteredUsers","AppOpens"]].sum()
    Map_user_groupby.reset_index(inplace= True)

    fig_line1 = px.line(Map_user_groupby,x= "States",y= ["RegisteredUsers","AppOpens"], hover_data= "States",
                            title= f"{year} {quarter}Q RegisterUsers, AppOpens", width= 1000,
                            markers= True, color_discrete_sequence= px.colors.sequential.Blugrn_r)
    st.plotly_chart(fig_line1)

# Map user states based District Regis user and app opens

def Map_user_plot2_state_Q(df,state, year,quarter):

    Tran_amo_cou_year = df[df["States"]== state]
    Tran_amo_cou_year.reset_index(drop= True, inplace= True)

    col1,col2 = st.columns(2)
    with col1:

        fig_bar = px.bar(Tran_amo_cou_year,x= "RegisteredUsers", y= "Districts",
                        orientation= "h", title= f"{state} {year} {quarter}Q RegisteredUsers", height= 800,
                        color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_bar)

    with col2:

        fig_bar2 = px.bar(Tran_amo_cou_year,x= "AppOpens", y= "Districts",
                        orientation= "h", title= f"{state} {year} {quarter}Q AppOpens", height= 800,
                        color_discrete_sequence=px.colors.sequential.Rainbow_r)

        st.plotly_chart(fig_bar2)



def Map_user_plot2_state_Y(df,state, year):

    Tran_amo_cou_year = df[df["States"]== state]
    Tran_amo_cou_year.reset_index(drop= True, inplace= True)

    col1,col2 = st.columns(2)
    with col1:

        fig_bar = px.bar(Tran_amo_cou_year,x= "RegisteredUsers", y= "Districts",
                        orientation= "h", title= f"{state} {year} RegisteredUsers", height= 800,
                        color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_bar)

    with col2:

        fig_bar2 = px.bar(Tran_amo_cou_year,x= "AppOpens", y= "Districts",
                        orientation= "h", title= f"{state} {year} AppOpens", height= 800,
                        color_discrete_sequence=px.colors.sequential.Rainbow_r)

        st.plotly_chart(fig_bar2)

# Top transaction district wise analysis

def Top_trans_Q_District(df,year,state):   
    Top_tran_state = df[df["States"]== state]
    Top_tran_state.reset_index(drop = True, inplace= True)

    Top_tran_groupby = Top_tran_state.groupby("Districts")[["Transaction_count","Transaction_amount"]].sum()
    Top_tran_groupby.reset_index(inplace= True)

    col1,col2 = st.columns(2)
    with col1:

        fig_bar = px.bar(Top_tran_groupby,x= "Transaction_count", y= "Districts",orientation="h",
                        hover_data= "Districts",hover_name= "Districts",title= f"{state} {year} Transaction_count", height= 300,
                        color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_bar)
    with col2:

        fig_bar2 = px.bar(Top_tran_groupby,x= "Transaction_amount", y= "Districts",orientation="h",
                        hover_data= "Districts",hover_name= "Districts",title= f"{state} {year} Transaction_amount", height= 300,
                        color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_bar2)

# Top transaction Pincode wise- quarter and Trans count,amount analysis

def Top_trans_Q_pincode(df,year,state): 
    Top_tran_state = df[df["States"]== state]
    Top_tran_state.reset_index(drop = True, inplace= True)

    col1,col2 = st.columns(2)
    with col1:
        fig_bar = px.bar(Top_tran_state,x= "Transaction_count", y= "Quarter", orientation= "h",
                        hover_name= "Pincode",title= f"{state} {year} Transaction_count", height= 300,
                        color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_bar)

    with col2:
        fig_bar2 = px.bar(Top_tran_state,x= "Transaction_amount", y= "Quarter",orientation="h",
                        hover_name= "Pincode",hover_data= "Transaction_amount",title= f"{state} {year} Transaction_amount", height= 300,
                        color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_bar2)

# Top user district wise state plot
        
def top_user_plot_state(df,year):
    top_user_groupby = pd.DataFrame(df.groupby(["States","Quarter"])["RegisteredUsers"].sum())
    top_user_groupby.reset_index(inplace= True)

    fig_bar1 = px.bar(top_user_groupby,x= "States", y= "RegisteredUsers", color= "Quarter", 
                        hover_name= "States",title= f"{year} Register Users", width= 1000,
                        height= 1000)
    st.plotly_chart(fig_bar1)

# Top user districtwise district and registered user analysis

def top_user_plot_district(df,year,state):
    Top_user_state = df[df["States"]== state]
    Top_user_state.reset_index(drop = True, inplace= True)

    fig_top_plot = px.bar(Top_user_state,x= "Quarter", y= "RegisteredUsers",
                        title= f"{year} {state} RegisteredUsers, District, Quarter",
                        width= 1000, height= 1000, color= "RegisteredUsers",
                        hover_data= "District", color_continuous_scale= px.colors.sequential.haline_r)

    st.plotly_chart(fig_top_plot)


# Top user districtwise district and registered user analysis

def top_user_plot2_district(df,year,state):
    Top_user_pincode = df[df["States"]== state]
    Top_user_pincode.reset_index(drop = True, inplace= True)

    fig_top_plot = px.bar(Top_user_pincode,x= "Quarter", y= "RegisteredUsers",
                        title= f"{year} {state} RegisteredUsers, Pincode, Quarter",
                        width= 1000, height= 1000, color= "RegisteredUsers",
                        hover_data= "Pincode", color_continuous_scale= px.colors.sequential.haline_r)

    st.plotly_chart(fig_top_plot)

def Top10_states_Trans(df,year):
    Top_trans_states_group = df.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    Top_trans_states_group.reset_index(inplace= True)

    Sort_TRSG = Top_trans_states_group.sort_values(by=['Transaction_amount'], ascending=False).head(10)
    Sort_TRSG.reset_index(drop= True)

    fig = px.pie(Sort_TRSG, values='Transaction_amount', names='States', title=f'{year} Top 10 States Vs Transaction Amount')
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig)

    # Top 10 districts in transaction amount

def Top10_districts_Trans(df,year):
    Top_trans_dist_group = df.groupby("Districts")[["Transaction_count","Transaction_amount"]].sum()
    Top_trans_dist_group.reset_index(inplace= True)

    Sort_TRDG = Top_trans_dist_group.sort_values(by= ["Transaction_amount"], ascending= False).head(10)
    Sort_TRDG.reset_index(drop= True,inplace= True)

    fig1 = px.pie(Sort_TRDG,values= "Transaction_amount", names= "Districts",title= f"{year} Top 10 Districts Vs Transaction Amount")
    fig1.update_traces(textposition= 'inside',textinfo= 'percent+label')
    st.plotly_chart(fig1)

# Top 10 pincodes in transaction amount

def Top10_pincodes_Trans(df,year):    
    Top_trans_pinc_group = df.groupby("Pincode")[["Transaction_count","Transaction_amount"]].sum()
    Top_trans_pinc_group.reset_index(inplace= True)

    Sort_TRPG = Top_trans_pinc_group.sort_values(by= ["Transaction_amount"], ascending= False).head(10) 
    Sort_TRPG.reset_index(drop= True,inplace= True)


    fig2 = px.pie(Sort_TRPG,values= "Transaction_amount", names= "Pincode",title= f"{year} Top 10 Pincodes Vs Transaction Amount")
    fig2.update_traces(textposition= 'inside',textinfo= 'percent+label')
    st.plotly_chart(fig2)



# Streamlit part

#st.markdown("<h1 style='text-align: center;'>Phonepe Data Visualization And Exploration</h1>", unsafe_allow_html=True)

st.set_page_config(layout= "wide")
st.title("Welcome To Phonepe Data Visualization And Exploration")
tab1,tab2,tab3 = st.tabs(["Home","Data Exploration","Top Charts"])

with tab1:
        st.subheader("Home Page")

with tab2:
    
        st.subheader("Data Exploration")
        tab11,tab12,tab13 = st.tabs(["Aggregated Analaysis", "Map Analaysis","Top Analaysis"])

        with tab11:
            
            method_1 = st.selectbox ("Select the method",["Select","Aggregated Transaction","Aggregated User"])
            
            if method_1 == "Aggregated Transaction":
                
                col1,col2 = st.columns(2)
                with col1:
                    years = st.selectbox("Select the year", ["Selet",2018,2019,2020,2021,2022,2023 ])
                
                    quarter = st.selectbox("Select the Quarter", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
                
                year_data = Transaction_year(years,Agg_transaction)
                
                if (quarter == "Whole Year"):
                    result = Transaction_amount_count_yearwise(years,Agg_transaction)
                else:
                    quarter = int(quarter[1])
                    Transaction_amount_count_quarter(quarter,year_data)
                
                quarter_data = Transaction_quarter_data(quarter,year_data)
                
                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("Select the State",year_data["States"].unique())
                    
                    if (quarter == "Whole Year"):
                        Aggre_Trans_Type_state_year(year_data,states)
                    else:    
                        Aggre_Trans_Type_state_quarter(quarter_data,states)

            elif method_1 == "Aggregated User":
                
                col1,col2 = st.columns(2)
                with col1:
                    years = st.selectbox("Select the year", Agg_user["Years"].unique() )
                
                    quarter = st.selectbox("Select the Quarter", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
            
                Agg_user_y_data = Transaction_year(years,Agg_user)
            
                Agg_user_quarter_data = Transaction_quarter_data(1,Agg_user_y_data)

                if (quarter == "Whole Year"):
                    Aggre_user_plot_year(Agg_user_y_data,years)
                else:
                    quarter = int(quarter[1])
                    Aggre_user_plot_quarter(Agg_user_quarter_data,years,quarter)
                    
                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("Select the State",Agg_user["States"].unique())
                    
                    if (quarter == "Whole Year"):
                        Aggre_user_plot_state_Y(Agg_user_y_data,years,states)
                    else:    
                        Aggre_user_plot_state_Q(Agg_user_quarter_data,years,states)
                
        
        with tab12:
            method_2 = st.selectbox ("Select the method",["Map Transaction","Map User"])
            
            if method_2 == "Map Transaction":
            
                col1,col2 = st.columns(2)
            
                with col1:
                    M_years = st.selectbox("Select the Year", ["Selet",2018,2019,2020,2021,2022,2023 ])
                
                    M_quarter = st.selectbox("Select the Quarter value", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
                
                M_tran_year_data = Transaction_year(M_years,Map_transaction)

                if (M_quarter == "Whole Year"):
                    Transaction_amount_count_yearwise(M_years,M_tran_year_data)
                else:
                    quarter = int(M_quarter[1])
                    M_tran_quarter_data = Transaction_quarter_data(quarter,M_tran_year_data)
                    Transaction_amount_count_quarter(quarter,M_tran_quarter_data)

                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("State Select",Map_transaction["States"].unique())

                if (M_quarter == "Whole Year"):
                    
                    Map_Transaction_district(M_tran_year_data,states)
                else:
                    quarter = int(M_quarter[1])
                    Map_Transaction_district(M_tran_quarter_data,states)

            elif method_2 == "Map User":
                col1,col2 = st.columns(2)
            
                with col1:
                    M_years = st.selectbox("Select the Year", ["Selet",2018,2019,2020,2021,2022,2023 ])
                
                    M_quarter = st.selectbox("Select the Quarter value", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
                
                M_user_year_data = Transaction_year(M_years,Map_user)

                if (M_quarter == "Whole Year"):
                    Map_user_plot_state(M_user_year_data, M_years)
                else:
                    quarter = int(M_quarter[1])
                    Map_user_quarter_data = Transaction_quarter_data(quarter,M_user_year_data)
                    Map_user_plot_state_Q(Map_user_quarter_data, M_years,quarter)

                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("State Select",Map_user["States"].unique())

                if (M_quarter == "Whole Year"):
                    
                    Map_user_plot2_state_Y(M_user_year_data,states,M_years)

                else:
                    quarter = int(M_quarter[1])
                    Map_user_plot2_state_Q(Map_user_quarter_data,states, M_years,quarter)

        with tab13:
            method_3 = st.selectbox ("Select the method",["Top Transaction Districtwise","Top Transaction Pincodewise","Top User Districtwise","Top User Pincodewise" ])
            
            if method_3 == "Top Transaction Districtwise":
            
                col1,col2 = st.columns(2)
            
                with col1:
                    T_years = st.selectbox("Select Year",  Top_transaction_districtwise["Years"].unique())
                
                    T_quarter = st.selectbox("Select Quarter", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
                
                Top_year_data = Transaction_year(T_years, Top_transaction_districtwise)
            

                if (T_quarter == "Whole Year"):
                    Transaction_amount_count_yearwise(T_years,Top_year_data)
                else:
                    
                    quarter = int(T_quarter[1])
                    Top_quarter_data = Transaction_quarter_data(quarter,Top_year_data)
                    Transaction_amount_count_quarter(quarter,Top_quarter_data)
                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("State select",Top_transaction_districtwise["States"].unique())

                    Top_trans_Q_District(Top_year_data,T_years,states)

            if method_3 == "Top Transaction Pincodewise":
            
                col1,col2 = st.columns(2)
            
                with col1:
                    T_years = st.selectbox("Select Year", Top_transaction_pincodewise["Years"].unique())
                
                    T_quarter = st.selectbox("Select Quarter", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
                
                Top_year_data = Transaction_year(T_years, Top_transaction_pincodewise)
            

                if (T_quarter == "Whole Year"):
                    Transaction_amount_count_yearwise(T_years,Top_year_data)
                else:
                    
                    quarter = int(T_quarter[1])
                    Top_quarter_data = Transaction_quarter_data(quarter,Top_year_data)
                    Transaction_amount_count_quarter(quarter,Top_quarter_data)
                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("State select",Top_transaction_pincodewise["States"].unique())

                    Top_trans_Q_pincode(Top_year_data,T_years,states)

            elif method_3 == "Top User Districtwise":
                
                col1,col2 = st.columns(2)
            
                with col1:
                    T_years = st.selectbox("Select Year",  Top_user_districtwise["Years"].unique())
                
                    #T_quarter = st.selectbox("Select Quarter", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
                
                Top_year_data = Transaction_year(T_years, Top_user_districtwise)

                top_user_plot_state(Top_year_data,T_years)
                
                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("State select",Top_transaction_pincodewise["States"].unique())
               
                    top_user_plot_district(Top_year_data,T_years,states)
                    
            elif method_3 == "Top User Pincodewise":

                col1,col2 = st.columns(2)
            
                with col1:
                    T_years = st.selectbox("Select Year",  Top_user_pincodewise["Years"].unique())                
        
                Top_year_data = Transaction_year(T_years, Top_user_pincodewise)

                top_user_plot_state(Top_year_data,T_years)
                
                col1, col2 = st.columns(2)

                with col1:
                    states = st.selectbox("State select",Top_transaction_pincodewise["States"].unique())
                    
                top_user_plot2_district(Top_year_data,T_years,states)
                    

with tab3:

        st.subheader("Top Charts")
        #tab1,tab2,tab3 = st.tabs(["States","Districts","Pincodes"])
        #col1,col2 = st.columns(2)
            
        #with col1:
        T_years = st.selectbox("Year Selection", Top_transaction_pincodewise["Years"].unique())
    
        T_quarter = st.selectbox("Quarter Selection", ["Q1 (Jan - Mar)","Q2 (Apr - Jun)","Q3 (Jul - Sep)","Q4 (Oct - Dec)", "Whole Year"])
    
        Top_year_data = Transaction_year(T_years, Top_transaction_districtwise)
    
        col11,col12,col13 = st.columns(3)
        with col11:
            if (T_quarter == "Whole Year"):
                Top10_states_Trans(Top_year_data,T_years)
            else:
                quarter = int(T_quarter[1])
                Top_quarter_data = Transaction_quarter_data(quarter,Top_year_data)       

                Top10_states_Trans(Top_quarter_data,T_years)

        with col12:
            if (T_quarter == "Whole Year"):
                Top10_districts_Trans(Top_year_data,T_years)
            else:
                quarter = int(T_quarter[1])
                Top_quarter_data = Transaction_quarter_data(quarter,Top_year_data)       

                Top10_districts_Trans(Top_quarter_data,T_years)
        with col13:
            t_year_data = Transaction_year(T_years, Top_transaction_pincodewise)

            if (T_quarter == "Whole Year"):
                Top10_pincodes_Trans(t_year_data,2023)
            else:
                quarter = int(T_quarter[1])
                T_quarter_data = Transaction_quarter_data(quarter,t_year_data)       

                Top10_pincodes_Trans(T_quarter_data,T_years)

            

