import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv



# Configure paths
INPUT_PATH = r"D:\ITI_dataEng\bigdata\project\mimic-iii-clinical-database-demo-1.4\\"
OUTPUT_DIR = r"D:\ITI_dataEng\bigdata\project\cleaned\\"

csv_files = [INPUT_PATH +'PATIENTS.csv', INPUT_PATH +'ADMISSIONS.csv', INPUT_PATH +'ICUSTAYS.csv', INPUT_PATH +'DIAGNOSES_ICD.csv']

for file in csv_files: # Read CSV
    df = pd. read_csv(file)
    

# Data cleaning
    if file == INPUT_PATH + 'PATIENTS.csv':
        df.to_csv(OUTPUT_DIR+ 'patients.csv', index=False)
        df.to_parquet(OUTPUT_DIR + 'patients.parquet', engine='pyarrow', index=False)

    # Step 1: Read with explicit dtype conversion
        table = pq.read_table(OUTPUT_DIR + 'patients.parquet')
        schema = pa.schema([
        pa.field('row_id', pa.int64()),
        pa.field('subject_id', pa.int64()),
        pa.field('gender', pa.string()),
        pa.field('dob', pa.timestamp('ns')),
        pa.field('dod', pa.timestamp('ns')),
        pa.field('dod_hosp', pa.timestamp('ns')),
        pa.field('dod_ssn', pa.timestamp('ns')),
        pa.field('expire_flag', pa.int32())     ]) # CRITICAL: Force INT32 
        

# Step 2: Write with hive-compatible settings
        pq.write_table(
        table.cast(schema),
        OUTPUT_DIR +'patients.parquet',
        version='2.6',
        use_dictionary=True,
        compression='SNAPPY' ,
        flavor='spark' )#Adds Hive/Spark-compatible metadata (timestamps, dictionaries)

    elif file == INPUT_PATH + 'ADMISSIONS.csv':
        #fill null text with unkonwn
        text_columns = ['language', 'religion', 'marital_status', 'ethnicity', 'diagnosis']
        df[text_columns] = df[text_columns].fillna('UNKNOWN')
        #convert to category
        cat_cols = ['admission_type', 'admission_location', 'discharge_location','insurance', 'language', 'religion', 'marital_status', 'ethnicity']
        df[cat_cols] = df[cat_cols].astype('category')
        #convert to datetime
        time_cols = ['admittime', 'dischtime', 'deathtime', 'edregtime', 'edouttime']
        for col in time_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        #convert to boolean 
        df['hospital_expire_flag'] = df['hospital_expire_flag'].astype(bool)
        df['has_chartevents_data'] = df['has_chartevents_data'].astype(bool)
        #Converts all values to uppercase and removes leading/trailing whitespace to avoid mismatches
        df['ethnicity'] = df['ethnicity'].str.upper().str.strip()
        #multiple granular or noisy categories into a smaller set of standardized groups
        df['ethnicity'] = df['ethnicity'].replace({
        'WHITE': 'WHITE',
        'WHITE - OTHER EUROPEAN': 'WHITE',
        'WHITE - EASTERN EUROPEAN': 'WHITE',
        'WHITE - BRAZILIAN': 'WHITE',
        'WHITE - RUSSIAN': 'WHITE',
        'BLACK/AFRICAN AMERICAN': 'BLACK',
        'BLACK/CARIBBEAN ISLAND': 'BLACK',
        'ASIAN': 'ASIAN',
        'HISPANIC OR LATINO': 'HISPANIC',
        'HISPANIC/LATINO - PUERTO RICAN': 'HISPANIC',
        'UNKNOWN/NOT SPECIFIED': 'UNKNOWN',
        'UNABLE TO OBTAIN': 'UNKNOWN'})
        # correct mairtal status
        df['marital_status'] = df['marital_status'].str.upper().str.strip()
        df['marital_status'] = df['marital_status'].replace({
        'UNKNOWN (DEFAULT)': 'UNKNOWN',
        'LIFE PARTNER': 'PARTNER'})
        #calc how long patients stayed at hosiptal?
        df['los_days_hos'] = (df['dischtime'] - df['admittime']).dt.days
        df['los_days_hos'] = df['los_days_hos'].round().astype(int)
        
        #Converts all values to uppercase and removes leading/trailing whitespace to avoid mismatches
        df['diagnosis'] = df['diagnosis'].str.upper().str.strip()
        # just extract date to be easy to group by date only
        df['admit_date'] = df['admittime'].dt.date
        df['discharge_date'] = df['dischtime'].dt.date
        df['admit_date'] = pd.to_datetime(df['admit_date'], errors='coerce')
        df['discharge_date'] = pd.to_datetime(df['discharge_date'], errors='coerce')
        #convert to csv after cleaned 
        df.to_csv(OUTPUT_DIR+ 'Admissions.csv', index=False)
        #convert to parquet 
        df.to_parquet(OUTPUT_DIR + 'admissions.parquet', engine='pyarrow', index=False)
        table = pq.read_table(OUTPUT_DIR + 'admissions.parquet')
        schema = pa.schema([
        pa.field('row_id', pa.int64()),
        pa.field('subject_id', pa.int64()),
        pa.field('hadm_id', pa.int64()),
        pa.field('admittime', pa.timestamp('ns')),
        pa.field('dischtime', pa.timestamp('ns')),
        pa.field('deathtime', pa.timestamp('ns')),
        pa.field('admission_type', pa.dictionary(pa.int8(), pa.string())),
        pa.field('admission_location', pa.dictionary(pa.int8(), pa.string())),
        pa.field('discharge_location', pa.dictionary(pa.int8(), pa.string())),
        pa.field('insurance', pa.dictionary(pa.int8(), pa.string())),
        pa.field('language', pa.dictionary(pa.int8(), pa.string())),
        pa.field('religion', pa.dictionary(pa.int8(), pa.string())),
        pa.field('marital_status', pa.dictionary(pa.int8(), pa.string())),
        pa.field('ethnicity', pa.dictionary(pa.int8(), pa.string())),
        pa.field('edregtime', pa.timestamp('ns')),
        pa.field('edouttime', pa.timestamp('ns')),
        pa.field('diagnosis', pa.string()),
        pa.field('hospital_expire_flag', pa.bool_()),
        pa.field('has_chartevents_data', pa.bool_()),
        pa.field('los_days_hos', pa.int64()),
        pa.field('admit_date', pa.timestamp('ns')),
        pa.field('discharge_date', pa.timestamp('ns')) ])
        

        pq.write_table(
        table.cast(schema),
        OUTPUT_DIR + 'admissions.parquet',
        version='2.6',
        use_dictionary=True,
        compression='SNAPPY', 
        )

        
    elif file == INPUT_PATH + 'ICUSTAYS.csv':
        df.drop(columns=['dbsource','first_careunit','last_careunit','first_wardid','last_wardid'],inplace=True)
        df['los_hours'] = df['los'] * 24 
        df['los_hours'] = df['los'].round().astype('int64')
        df.drop(columns=['los'],inplace=True)
         #convert to csv after cleaned 
        df.to_csv(OUTPUT_DIR+ 'ICUSTAYS.csv', index=False)
        #convert to parquet 
        df.to_parquet(OUTPUT_DIR + 'ICUSTAYS.parquet', engine='pyarrow', index=False)
        table = pq.read_table(OUTPUT_DIR + 'ICUSTAYS.parquet')

        schema = pa.schema([
        ('row_id', pa.int64()),
        ('subject_id', pa.int64()),
        ('hadm_id', pa.int64()),
        ('icustay_id', pa.int64()),
        ('intime', pa.timestamp('ms')),
        ('outtime', pa.timestamp('ms')),
        ('los_hours',pa.int64()) ])


        pq.write_table(table.cast(schema),
        OUTPUT_DIR +'ICUSTAYS.parquet',
        version='2.6',
        use_dictionary=True,
        compression='SNAPPY' ,
        flavor ='spark' )#Adds Hive/Spark-compatible metadata (timestamps, dictionaries)

    elif file == INPUT_PATH + 'DIAGNOSES_ICD.csv':
        df.to_parquet(OUTPUT_DIR +'diagnoses_icd.parquet',engine= 'pyarrow',index=False )
        # Step 1: Read with explicit dtype conversion
        table = pq.read_table(OUTPUT_DIR + 'diagnoses_icd.parquet')
        schema = pa.schema([
        pa.field('row_id', pa.int64()),
        pa.field('subject_id', pa.int64()),
        pa.field('hadm_id', pa.int64()), 
        pa.field('seq_num', pa.int64()),
        pa.field('icd9_code', pa.string()) ])

# Step 2: Write with hive-compatible settings
        pq.write_table(
        table.cast(schema),
        OUTPUT_DIR +'diagnoses_icd.parquet',
        version='2.6',
        use_dictionary=True,
        compression='SNAPPY')





