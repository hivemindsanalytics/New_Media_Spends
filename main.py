# This is a sample Python script.
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from datetime import date
import psycopg2
import streamlit.components.v1 as components
from sqlalchemy import types
import sqlalchemy
from sqlalchemy import create_engine
import base64
import pytz
import io
from io import BytesIO
import itertools
tz = pytz.timezone('Asia/Kolkata')
from dateutil.relativedelta import relativedelta

st.set_page_config(page_title="Hiveminds",page_icon=":smiley:",layout="wide")


def read_db(tablename,type):
    postgre_url = "postgresql://{user}:{pw}@{host}/{db}".format(user="hiveminds",
                                                                pw="tr0ub4dor&3",
                                                                host="173.255.202.215",
                                                                db='media_db',
                                                                port=5432)
    engine = create_engine(postgre_url, pool_pre_ping=True, max_overflow=-1, echo=False)
    if type == 'query' or type == 'select-query':
        query = tablename
    elif type == 'table':
        query = (f"SELECT * FROM {tablename};")

    connection = engine.raw_connection()

    if type == 'query':
        cur = connection.cursor()
        cur.execute(query)
        connection.commit()
        connection.close()
        return "Successful"
    elif type == 'table' or type == 'select-query':
        result = pd.read_sql_query(query, connection)
        connection.commit()
        connection.close()
        return result

def push_data(df,dbname):
    engine = create_engine("postgresql://{user}:{pw}@{host}/{db}".format(user="hiveminds",
                                                                         pw="tr0ub4dor&3",
                                                                         host="173.255.202.215",
                                                                         db='media_db',port=5432), pool_pre_ping=True, max_overflow=-1, echo=False)
    df.head(0).to_sql(dbname, engine, if_exists='append', index=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output,dbname, null="")  # null values become ''
    conn.commit()
    return "Success"

def app():
    html_temp = """
            <div style="background-color:lightskyblue;padding:0px">
            <h1 style="color:white;text-align:center;"><img src="https://bestmediainfo.com/timthumb.php?src=/wp-content/uploads/2019/05/HiveMinds_2.jpg&w=620&h=350&zc=1&q=100" width="120" height="80" alt="Natural" alt="logo" /> MEDIA DATA</h1>
            </div>
            """
    sidebarbackground = """
        <style>
        .sidebar .sidebar-content {
            background-image: linear-gradient(#87CEFA,#87CEFA);
            background-color: transparent;
            color: white;}
        """

    hide_footer_style = """
                <style>
                .reportview-container .main footer {visibility: hidden;}    
                """
    st.markdown(hide_footer_style, unsafe_allow_html=True)

    st.markdown(sidebarbackground, unsafe_allow_html=True)

    st.markdown(html_temp, unsafe_allow_html=True)


    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type='password')

    if st.sidebar.checkbox("Login"):

        if username == 'hiveminds' and password == 'Hive@123#':

            acc_mapping = read_db('account_mapping', 'table')
            today = datetime.now(tz=tz).date()+timedelta(1)
            lastmonth = today - relativedelta(months=1)+timedelta()

            mth = [today.strftime('%m-%Y'), lastmonth.strftime('%m-%Y')]

            clients = acc_mapping['client_name'].unique()

            activities = ["Add data", "View Data","Approve Data","Download Data","Add Planned Spends"]
            choice = st.sidebar.selectbox("Select Option :- ", activities)
            if choice == 'Add data':

                col1,col2,col3,col4 = st.beta_columns([1,1,1,1])
                with col1:
                    client_selected = st.selectbox("Select Client", clients)

                with col2:
                    platforms = acc_mapping['platform'][acc_mapping['client_name']==client_selected].unique()
                    platform_selected = st.selectbox("Select Platform",platforms)
                with col3:
                    Month_selected = st.selectbox("Select Month",mth)
                with col4:
                    if Month_selected == lastmonth.strftime('%m-%Y'):
                        Spend_Options = ['Actuals']
                    elif Month_selected == today.strftime('%m-%Y'):
                        Spend_Options = ['Estimate']
                    elif Month_selected == today.strftime('%m-%Y'):
                        Spends_Options = ['Tentative Actuals']

                    Spend_Type_selected = st.selectbox("Select Spends type", Spend_Options)



                if st.checkbox("Download template"):

                    channel_mapping = read_db('channel_mapping', 'table')

                    if platform_selected == 'Marketplace' or platform_selected == 'Marketplace-International':
                        channels = channel_mapping[channel_mapping['service'].isin(['AMS','Flipkart'])]['channel'].tolist()

                    elif platform_selected == 'Affiliate':
                        channels = channel_mapping[channel_mapping['service'].isin(['Affiliate'])]['channel'].tolist()

                    else:
                        channels = channel_mapping[~channel_mapping['service'].isin(['AMS', 'Flipkart','Affiliate'])]['channel'].tolist()

                    sample = pd.DataFrame(columns=['Client', 'Type of Client','Channel','Month','Spends'])
                    # channels = ['Google search', 'PLA', 'UAC Display', 'UAC Video', 'UAC (Text)', 'Bing', 'FB',
                    #             'Other Marketplaces', 'AMS(Non-India)', 'Flipkart',
                    #             'AMS', 'GDN(Rm incl)', 'Youtube+GDN Video', 'Programmatic-DV360','Programmatic-Inmobi','Programmatic-MediaSmart','Programmatic-Others', 'Affiliate Media', 'OTT-Hotstar',
                    #             'OTT-Sonyliv', 'OTT-Others',
                    #             'Jio', 'Native-Taboola, ContentDB, Studeyo, Outbrain etc',
                    #             'Online Newspapers/Content Aggregators-(Inshorts, Dailyhunt, Hindu, TOI etc) - Direct buys(Non-Programmatic/Non-GDN)',
                    #             'Audio Buys(Spotify, Gaana etc)', 'Third party media', 'Twitter', 'Linkedin', 'Quora',
                    #             'Apple Search','Yandex Search','Yandex Display','Metasearch Search','Metasearch Display','Grofers','bigbasket','1mg']
                    sample['Channel'] = channels
                    sample['Month'] = Month_selected
                    sample['Client'] = client_selected
                    sample['Type of Client'] = platform_selected
                    sample['Spends']= 0
                    sample['Spends'] = sample['Spends'].astype('float64')
                    sample['spends_type'] = Spend_Type_selected
                    sample['Download time'] = datetime.now()

                    output = BytesIO()
                    writer = pd.ExcelWriter(output, engine='xlsxwriter')


                    sample.to_excel(writer, sheet_name='Sheet1', index=False)
                    workbook = writer.book
                    worksheet= writer.sheets['Sheet1']
                    # worksheet.set_column('A:A', None, None, {'hidden': True})

                    # Adding lock/unlock capabilities.
                    locked_format = workbook.add_format()
                    locked_format.set_locked(True)
                    unlocked_format = workbook.add_format({'num_format': '0'})
                    unlocked_format.set_locked(False)

                    # Adjust column width
                    worksheet.set_column(2,2,40)





                    # Unlocking columns that need to remain unlocked.
                    worksheet.set_column('E:E', None, unlocked_format)



                    # Enable worksheet protection.
                    worksheet.protect('passprotect123',{'select_locked_cells':False})


                    # Lock columns that need to remain locked.
                    worksheet.set_column('A:D', None, locked_format)
                    worksheet.set_column('F:XFD', None, locked_format)
                    worksheet.set_column('G:XFD', None, locked_format,{'hidden':True})



                    writer.save()
                    processed_data = output.getvalue()
                    file_name = client_selected+'_'+Month_selected+'_'+Spend_Type_selected
                    b64 = base64.b64encode(processed_data)
                    href = f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="{file_name}.xlsx">Download template file</a>'  # decode b'abc' => abc
                    st.markdown(href, unsafe_allow_html=True)


                    data = st.file_uploader(label="Upload spends file (template above)",type="xlsx")



                    if st.checkbox("Upload File and see preview"):
                        try:
                            df = pd.read_excel(data)
                            df['Spends'] = df['Spends'].astype('float64')
                            st.write(df.drop('Download time',axis=1))
                        except:

                            st.error("Invalid File")
                            st.info("1. Please make sure you have downloaded the above template and uploaded")
                            st.info("2. Please make sure spends does not contain commas or is not of type currency (Rs./₹ preceeding spends)")


                        else:
                            if st.checkbox("Upload data to DB"):
                                try:

                                    df['Spends'] = df['Spends'].astype('float64')
                                    df['Month'] = df['Month'].apply(lambda x: '01-'+x)
                                    df['Month'] = pd.to_datetime(df['Month'],format='%d-%m-%Y')
                                    df['year_month'] = df['Month'].dt.strftime('%Y-%b')
                                    df['year'] = df['Month'].dt.strftime('%Y').astype('int')
                                    df['month_name'] = df['Month'].dt.strftime('%b')

                                    cli = str(df['Client'].iloc[0])
                                    cli = cli.replace("'","''")
                                    df = df[['Client','Type of Client','Channel','Month','Spends','spends_type','year_month','year','month_name','Download time']]




                                    query = f"select * from public.media_data where client like '{cli}' and \"Type of Client\" = '{df['Type of Client'].iloc[0]}' and year_month= '{df['year_month'].iloc[0]}' and spends_type = '{df['spends_type'].iloc[0]}'"
                                    check = read_db(query, 'select-query')

                                    if len(check) > 0:
                                        st.error("Data already exists!")
                                    else:
                                        res = push_data(df,'media_data')
                                        if res == "Success":
                                            st.success("Database updated")
                                            st.info("Please refresh the link to update new data")

                                except:
                                    st.error("Error!")
                                    st.info("1. Please make sure you have downloaded the above template and uploaded")
                                    st.info("2. Please make sure spends does not contain commas or is not of type currency (Rs./₹ preceeding spends)")

            elif choice == "View Data":

                col5, col6, col7, col8 = st.beta_columns([1, 1, 1, 1])
                with col5:
                    client_selected = st.selectbox("Select Client", clients)

                with col6:
                    platforms = acc_mapping['platform'][acc_mapping['client_name'] == client_selected].unique()
                    platform_selected = st.selectbox("Select Platform", platforms)
                with col7:
                    Month_selected = st.selectbox("Select Month", mth)
                    dt = datetime.strptime(Month_selected, '%m-%Y').date()

                with col8:
                    if Month_selected == lastmonth.strftime('%m-%Y'):
                        Spend_Options = ['Actuals']
                    elif Month_selected == today.strftime('%m-%Y'):
                        Spend_Options = ['Estimate']

                    Spend_Type_selected = st.selectbox("Select Spends type", Spend_Options)


                if st.checkbox("View Data"):
                    media_data = read_db('media_data', 'table')
                    media_data = media_data[(media_data['client'] == client_selected) & (media_data['Type of Client'] == platform_selected) & (media_data['spends_type'] == Spend_Type_selected) & (media_data['month'] == dt)]
                    st.write(media_data.drop(['year','month_name','downloadtime'],axis=1))
                    media_csv = media_data.to_csv(index=False)
                    b64 = base64.b64encode(media_csv.encode()).decode()  # some strings <-> bytes conversions necessary here
                    href = f'<a href="data:file/csv;base64,{b64}" download="download.csv">Download CSV File</a>'
                    st.markdown(href, unsafe_allow_html=True)

            # elif choice == "Delete Data":
            #     mth_options = [today.strftime('%Y-%b'), lastmonth.strftime('%Y-%b')]
            #     col9, col10, col11,col12 = st.columns([1, 1, 1,1])
            #     with col9:
            #         client_selected = st.selectbox("Select Client", clients)
            #
            #     with col10:
            #         platforms = acc_mapping['platform'][acc_mapping['client_name'] == client_selected].unique()
            #         platform_selected = st.selectbox("Select Platform", platforms)
            #     with col11:
            #         Month_selected = st.selectbox("Select Month", mth_options)
            #         dt = datetime.strptime(Month_selected,'%Y-%b').date()
            #
            #     with col12:
            #         # if Month_selected == lastmonth.strftime('%Y-%b'):
            #         #     Spend_Options = ['Actuals']
            #         # elif Month_selected == today.strftime('%Y-%b'):
            #         #     Spend_Options = ['Estimate']
            #         Spend_Options = ['Actuals','Estimate']
            #
            #         Spend_Type_selected = st.selectbox("Select Spends type", Spend_Options)
            #
            #
            #     if st.checkbox("Delete Data?"):
            #         if st.checkbox("Confirm Deletion"):
            #             query = f"select * from public.media_data where \"Client\" = '{client_selected}' and \"Type of Client\" = '{platform_selected}' and year_month= '{Month_selected}' and spends_type = '{Spend_Type_selected}'"
            #             check = read_db(query, 'select-query')
            #             if len(check) ==0:
            #                 st.error("No data in DB to delete!")
            #             else:
            #                 query = f"delete from public.media_data where \"Client\" = '{client_selected}' and \"Type of Client\" = '{platform_selected}' and year_month= '{Month_selected}' and spends_type = '{Spend_Type_selected}'"
            #                 status = read_db(query, 'query')
            #                 if status == 'Successful':
            #                     st.success("Deleted!!!")
            #                 else:
            #                     st.error('Error in deleting!!')

            elif choice == "Approve Data":
                col13,col14 = st.beta_columns([1,1])

                with col13:
                    username1 = st.text_input("Enter Username")

                with col14:
                    password1 = st.text_input("Enter Password", type='password')

                if st.checkbox("Sign in"):

                    if username1 == 'admin' and password1 == 'Hiveadmin@123#':

                        mth_options = [today.strftime('%Y-%b'), lastmonth.strftime('%Y-%b')]
                        col15, col16, col17, col18 = st.beta_columns([1, 1, 1,1])
                        with col15:
                            dulist = acc_mapping['du_name'].unique()
                            du_selected = st.selectbox("Select DU", dulist)

                            # if Month_selected == lastmonth.strftime('%Y-%b'):
                            #     Spend_Options = ['Actuals']
                            # elif Month_selected == today.strftime('%Y-%b'):
                            #     Spend_Options = ['Estimate']
                            Spend_Options = ['Actuals', 'Estimate']

                            Spend_Type_selected = st.selectbox("Select Spends type", Spend_Options)

                        with col16:
                            clientlist = acc_mapping[acc_mapping['du_name'] == du_selected]['client_name'].unique()
                            client_selected = st.multiselect("Select Client(s)", clientlist)
                            client_selected1 = [str(x).replace("'","''") for x in client_selected]
                            # print(client_selected1)

                        with col17:
                            platforms = acc_mapping['platform'][acc_mapping['client_name'].isin(client_selected)].unique()
                            platform_selected = st.multiselect("Select Platform(s)", platforms)


                        with col18:
                            Month_selected = st.selectbox("Select Month", mth_options)
                            dt = datetime.strptime(Month_selected, '%Y-%b').date()

                        if st.checkbox("Fetch Data"):
                            comb = tuple(itertools.product(client_selected1,platform_selected))
                            print(comb)

                            if len(comb) == 1:
                                query = f"select * from public.media_data left join public.account_mapping on client_name = client and platform = \"Type of Client\"  where client = '{comb[0][0]}' and \"Type of Client\" = '{comb[0][1]}' and month= '{dt}' and spends_type = '{Spend_Type_selected}' and \"du_name\" = '{du_selected}'"
                            else:
                                query = f"select * from public.media_data left join public.account_mapping on client_name = client and platform = \"Type of Client\"   where (client,\"Type of Client\") in {comb} and month= '{dt}' and spends_type = '{Spend_Type_selected}' and \"du_name\" = '{du_selected}'"




                            df = read_db(query, 'select-query')

                            if len(df) == 0:
                                st.info("No data to display")
                            else:
                                df1=df.groupby(['client','Type of Client']).sum()['spends'].reset_index()
                                df2 = pd.pivot_table(df1, values='spends', index=['client'],columns='Type of Client').reset_index().fillna(0)
                                df2['Total'] = df2.iloc[:,1:].sum(axis=1)
                                st.write(df2)

                                if st.checkbox("Approve Data"):
                                    st.info("Kindly recheck if the below data has to be approved")
                                    st.write(df2)
                                    if st.checkbox("Confirm approval"):
                                        df1['year_month'] = Month_selected
                                        df1['spends_type'] = Spend_Type_selected
                                        df1['du_name'] = du_selected
                                        df1['approval'] = 'approved'

                                        df1 = df1[['client','Type of Client','year_month','spends_type','du_name','approval']]

                                        comb = tuple(itertools.product(client_selected1, platform_selected))

                                        if len(comb) == 1:
                                            query = f"select * from public.approval_data  where client = '{comb[0][0]}' and \"Type of Client\" = '{comb[0][1]}' and \"year_month\"= '{Month_selected}' and spends_type = '{Spend_Type_selected}' and \"du_name\" = '{du_selected}'"
                                        else:
                                            query = f"select * from public.approval_data where (client,\"Type of Client\") in {comb} and \"year_month\"= '{Month_selected}' and spends_type = '{Spend_Type_selected}' and \"du_name\" = '{du_selected}'"

                                        check = read_db(query, 'select-query')
                                        if len(check) > 0:
                                            st.error("Above Data already approved!!")
                                        else:
                                            res = push_data(df1,'approval_data')
                                            if res == "Success":
                                                st.success("Database updated")
                                                st.info("Please refresh the link to update new data")

                    else:
                        st.error("Invalid Username or Password!")

            elif choice == "Download Data":
                col13,col14 = st.beta_columns([1,1])

                with col13:
                    username1 = st.text_input("Enter Username")

                with col14:
                    password1 = st.text_input("Enter Password", type='password')

                if st.checkbox("Sign in"):

                    if username1 == 'admin' and password1 == 'Superadmin@123#':

                        months_query = "select distinct Month from public.media_data where Month <= CURRENT_DATE order by Month desc"

                        months = read_db(months_query, 'select-query')
                        col19, col20 = st.beta_columns([1, 1])
                        with col19:
                            months_selected = st.multiselect("Select Month(s)",months)
                            mon_str = ''
                            for month in months_selected:
                                m = "'"+month.strftime("%Y-%b")+"'"+","
                                mon_str = mon_str+str(m)

                        with col20:
                            Spend_Options = ['Actuals', 'Estimate']
                            Spend_Type_selected = st.multiselect("Select Spends type", Spend_Options)

                            spend_sel = ''
                            for spend_type in Spend_Type_selected:
                                s = "'" + spend_type + "'" + ","
                                spend_sel = spend_sel + str(s)



                        if st.checkbox("Fetch Data"):

                            query = f"""select md.*,am.delivery_vertical,am.du_name,am.poc_name,ad.approval
                                        from public.media_data md
                                        left join public.account_mapping am on md.client = am.client_name and md.\"Type of Client\" = am.platform
                                        left join public.approval_data ad on ad.client = md.client and ad.\"Type of Client\" = md.\"Type of Client\"
                                        and ad.year_month = md.year_month and ad.spends_type = md.spends_type
                                        where  md.year_month in ({mon_str[:-1]})
                                        and md.spends_type in ({spend_sel[:-1]})
                                        """
                            df = read_db(query, 'select-query')



                            if len(df) == 0:
                                st.info("No data to display")
                            else:

                                df['spends'].fillna(0,inplace=True)
                                df['approval'].fillna('not approved', inplace=True)
                                df.drop('downloadtime',axis=1,inplace=True)
                                st.write(df)

                                csv = df.to_csv(index=False)
                                b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
                                href = f'<a href="data:file/csv;base64,{b64}" download="download.csv">Download CSV File</a>'
                                st.markdown(href, unsafe_allow_html=True)


                    else:
                        st.error("Invalid Username or Password!")

            elif choice == "Add Planned Data":
                if st.checkbox("Download Planned Data template"):
                    sample


        else:
            st.error("Invalid Username or Password!")









if __name__ == "__main__":
    app()
