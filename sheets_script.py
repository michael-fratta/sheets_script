# python scheduler
import schedule
import time

def job():

    ### set up Google API stuff

    import httplib2
    import os
    from apiclient import discovery
    from google.oauth2 import service_account
    import json
    from dotenv import load_dotenv
    import os
    load_dotenv()

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file']

    secret_file = json.loads(os.getenv('secret'))

    credentials = service_account.Credentials.from_service_account_info(secret_file, scopes=scopes)

    service = discovery.build('sheets','v4',credentials=credentials)


    ### map spreadsheet IDs ###

    in_life_spreadsheet_id = os.getenv('in_life_id')

    ops_reporting_spreadsheet_id = os.getenv('ops_reporting_id')


    ### define spreadsheet ranges ###

    road_tax_range = '[TABNAME] !A2:H'

    mileage_checks_range = '[TABNAME]!A2:G'

    cherished_plates_range = '[TABNAME]!A2:F'

    ops_orders_range = '[TABNAME]!J4:AE'

    insurance_expiry_range = '[TABNAME]!A2:H'

    mot_checks_range = '[TABNAME]!A2:D'

    edec_expiry_range = '[TABNAME]!A2:F'


    # import various files from SFTP

    import pandas as pd
    from datetime import datetime
    import pysftp

    today_half_hour = datetime.today().strftime("%d%m%Y %H-30")

    today = datetime.today().strftime("%d%m%Y")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys=None

    hostname = os.getenv('sftp_host')
    username = os.getenv('sftp_user')
    password = os.getenv('sftp_pass')

    ### get doc1.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc1{today}.csv") as f:
                print(f"Successfully read: doc1{today}.csv")
                road_tax_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc1{today}.csv")
            road_tax_df = pd.DataFrame()

    ### get doc2.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc2{today}.csv") as f:
                print(f"Successfully read: doc2{today}.csv")
                mileage_checks_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc2{today}.csv")
            mileage_checks_df = pd.DataFrame()

    ### get doc3.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc3{today}.csv") as f:
                print(f"Successfully read: doc3{today}.csv")
                cherished_plates_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc3{today}.csv")
            cherished_plates_df = pd.DataFrame()

    ### get doc4.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc4({today_half_hour}).csv") as f:
                print(f"Successfully read: doc4({today_half_hour}).csv")
                ops_orders_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc4({today_half_hour}).csv")
            ops_orders_df = pd.DataFrame()

    ### get doc5.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc5{today}.csv") as f:
                print(f"doc5{today}.csv")
                insurance_expiry_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc5{today}.csv")
            insurance_expiry_df = pd.DataFrame()

    ### get doc6.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc6({today}).csv") as f:
                print(f"doc6({today}).csv")
                mot_checks_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc6({today}).csv")
            mot_checks_df = pd.DataFrame()

    ### get doc7.csv ###
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("\nConnection succesfully established ... ")
        try:
            with sftp.open(f"doc7{today}.csv") as f:
                print(f"Successfully read: doc7{today}.csv")
                edec_expiry_df = pd.read_csv(f)
        except:
            print(f"Could not find/is blank: doc7{today}.csv")
            edec_expiry_df = pd.DataFrame()


    ### ready the files and submit to Gsheets ###

    ### for doc1.csv ###
    if not road_tax_df.empty:
        # convert dataframe to string for api call data payload
        road_tax_df2 = road_tax_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        road_tax_df3 = road_tax_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        road_tax_data = {'values': road_tax_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=in_life_spreadsheet_id, body=body, range=road_tax_range).execute()
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=in_life_spreadsheet_id, body=road_tax_data, range=road_tax_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc1{today}.csv is either blank, or could not be found!\n")

    ### for doc2.csv ###
    if not mileage_checks_df.empty:
        # convert dataframe to string for api call data payload
        mileage_checks_df2 = mileage_checks_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        mileage_checks_df3 = mileage_checks_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        mileage_checks_data = {'values': mileage_checks_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=in_life_spreadsheet_id, body=body, range=mileage_checks_range).execute()
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=in_life_spreadsheet_id, body=mileage_checks_data, range=mileage_checks_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc2{today}.csv is either blank, or could not be found!\n")

    ### for doc3.csv ###
    if not cherished_plates_df.empty:
        # convert dataframe to string for api call data payload
        cherished_plates_df2 = cherished_plates_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        cherished_plates_df3 = cherished_plates_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        cherished_plates_data = {'values': cherished_plates_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=in_life_spreadsheet_id, body=body, range=cherished_plates_range).execute()        
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=in_life_spreadsheet_id, body=cherished_plates_data, range=cherished_plates_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc3{today}.csv is either blank, or could not be found!\n")

    ### for doc4.csv ###
    if not ops_orders_df.empty:
        # convert dataframe to string for api call data payload
        ops_orders_df2 = ops_orders_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        ops_orders_df3 = ops_orders_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        ops_orders_data = {'values': ops_orders_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=ops_reporting_spreadsheet_id, body=body, range=ops_orders_range).execute()      
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=ops_reporting_spreadsheet_id, body=ops_orders_data, range=ops_orders_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc4({today_half_hour}).csv is either blank, or could not be found!\n")

    ### for doc5.csv ###
    if not insurance_expiry_df.empty:
        # convert dataframe to string for api call data payload
        insurance_expiry_df2 = insurance_expiry_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        insurance_expiry_df3 = insurance_expiry_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        insurance_expiry_data = {'values': insurance_expiry_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=in_life_spreadsheet_id, body=body, range=insurance_expiry_range).execute()      
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=in_life_spreadsheet_id, body=insurance_expiry_data, range=insurance_expiry_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc5{today}.csv is either blank, or could not be found!\n")

    ### for doc6.csv ###
    if not mot_checks_df.empty:
        # convert dataframe to string for api call data payload
        mot_checks_df2 = mot_checks_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        mot_checks_df3 = mot_checks_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        mot_checks_data = {'values': mot_checks_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=in_life_spreadsheet_id, body=body, range=mot_checks_range).execute()   
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=in_life_spreadsheet_id, body=mot_checks_data, range=mot_checks_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc6({today}).csv is either blank, or could not be found!\n")

    ### for doc7.csv ###
    if not edec_expiry_df.empty:
        # convert dataframe to string for api call data payload
        edec_expiry_df2 = edec_expiry_df.astype(str)
        # replace nans and NaTs with empty string for cleaner sheets
        edec_expiry_df3 = edec_expiry_df2.replace(to_replace=["nan","NaT"],value=" ")
        # assign data payloads for api call
        edec_expiry_data = {'values': edec_expiry_df3.values.tolist()}
        # clear Gsheets
        body = {}
        service.spreadsheets().values().clear(spreadsheetId=in_life_spreadsheet_id, body=body, range=edec_expiry_range).execute()   
        # update Gsheets
        service.spreadsheets().values().update(spreadsheetId=in_life_spreadsheet_id, body=edec_expiry_data, range=edec_expiry_range, valueInputOption='USER_ENTERED').execute()
        print(f"[TABNAME] updated!")
    else:
        print(f"doc7{today}.csv is either blank, or could not be found!\n")

    print("Job done!")

# run script every hour at 35mins past
schedule.every().hour.at(":35").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)