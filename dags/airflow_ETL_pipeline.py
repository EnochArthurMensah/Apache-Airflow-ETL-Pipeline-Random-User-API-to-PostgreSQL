from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

# importing the necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import requests
from datetime import datetime, date
from time import sleep

# extract function
def extract(limit):
    '''Extract data from random user API'''
    try:
        count=1
        data=[]
        header={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'}
        url='https://randomuser.me/api'
        while count <= limit:
            response=requests.get(url,header)
            if response.status_code == 200:
                response_json=response.json()
                data.append(response_json)
            else:
                break
            count +=1
            sleep(1) # delay for a second before it loops through again
    except Exception as e:
        print(e)
    return data    


# transform function
def transform(data):
    '''Create dataframe and perform some transformation on the dataframe '''
    # creating an empty list 
    first_name=[]
    last_name=[]
    gender=[]
    email=[]
    dob=[]
    country=[]
    street_address=[]
    city=[]
    state=[]
    postcode=[]
    phone=[]
    cell=[]
    users={}
    # loop through the data to get each user's record
    try:
        for user in data:
            first_name.append(user['results'][0]['name']['first'])
            last_name.append(user['results'][0]['name']['last'])
            gender.append(user['results'][0]['gender'])
            email.append(user['results'][0]['email'])
            dob.append(((user['results'][0]['dob']['date'])[0:10]))
            country.append(user['results'][0]['location']['country'])
            street_number=(str(user['results'][0]['location']['street']['number']))
            street_name=user['results'][0]['location']['street']['name']
            street_address.append(street_number + ' ' + street_name)
            city.append(user['results'][0]['location']['city'])
            state.append(user['results'][0]['location']['state'])
            postcode.append(user['results'][0]['location']['postcode'])
            phone.append((user['results'][0]['phone']).replace('(','').replace(')','').replace(' ','-'))
            cell.append((user['results'][0]['cell']).replace('(','').replace(')','').replace(' ','-'))
            users={'first_name':first_name,'last_name':last_name,'gender':gender,'email':email,'date_of_birth':dob,'country':country,'street_address':street_address,'city':city,'state':state,'postcode':postcode,'phone':phone,'cell':cell}
    except Exception as e:
        print(e)
    else:
        # creating a dataframe 
        dataframe=pd.DataFrame(users) 
        # information about the dataframe
        dataframe.info()
        # dropping duplicates using the email column
        dataframe.drop_duplicates(subset='email',inplace=True)
        # converting column datatype from object to datetime64
        # dataframe['date_of_birth']=pd.to_datetime(dataframe['date_of_birth'])
        return dataframe.to_json()

# load function()
def load(dataframe):
    '''Load the data into a database'''
    try:
        # creating connection to the database
        engine=create_engine('postgresql://airflow:airflow@host.docker.internal:5436/users')
        engine.connect()
    except Exception as e:
        print(e)
    else: 
        # loading into the postgre database
        dataframe_df=pd.read_json(dataframe)
        dataframe_df.to_sql('user_records',con=engine,if_exists='append', index=False)
    









default_args={
    'owner': 'Enoch Arthur-Mensah',
    'retries':2,
    'start_date':datetime(2023,8,26,8),
    'retry_delay':timedelta(minutes=2)
    }


with DAG(
    dag_id='Apache_Airflow_ETL_Pipeline',
    description='a pipeline for extracting, transforming and load of data from Random User API to PostgreSQL',
    schedule_interval='0 8 * * 1-7',
    default_args = default_args
    ) as dag:

    task1=PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_args=[500]
    )

    task2=PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_args=[task1.output]
    )

    task3=PythonOperator(
        task_id='load',
        python_callable=load,
        op_args=[task2.output]
    )

    task1>>task2>>task3