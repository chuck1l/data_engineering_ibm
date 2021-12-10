import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime


tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored


# Add the CSV extract function below
def deal_extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


# Add the JSON extract function below
def deal_extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe


# Add the XML extract function below, it is the same as the xml extract function above but the column names need to be renamed.
def deal_extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        model = car.find("car_model").text
        year = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        dataframe = dataframe.append({"car_model":model, "year_of_manufacture":year, "price":price, "fuel":fuel}, ignore_index=True)
    return dataframe


def deal_extract():
    deal_extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("./dealership_data/*.csv"):
        deal_extracted_data = deal_extracted_data.append(deal_extract_from_csv(csvfile), ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("./dealership_data/*.json"):
        deal_extracted_data = deal_extracted_data.append(deal_extract_from_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("./dealership_data/*.xml"):
        deal_extracted_data = deal_extracted_data.append(deal_extract_from_xml(xmlfile), ignore_index=True)
        
    return deal_extracted_data


# Add the transform function below
def deal_transform(data):
        # Round the price column to 2 decimal places
        data['price'] = round(data.price,2)
        return data


# Add the load function below
def deal_load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile, index=False)  


# Add the log function below
def deal_log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


# Perform the ETL Process

# Log that you have started the ETL process
deal_log("ETL Job Started")

# Log that you have started the Extract step
deal_log("Extract phase Started")

# Call the Extract function
extracted_data = deal_extract()

# Log that you have completed the Extract step
deal_log("Extract phase Ended")

# Log that you have started the Transform step
deal_log("Transform phase Started")

# Call the Transform function
transformed_data = deal_transform(extracted_data)
# Log that you have completed the Transform step
deal_log("Transform phase Ended")

# Log that you have started the Load step
deal_log("Load phase Started")

# Call the Load function
deal_load(targetfile,transformed_data)
# Log that you have completed the Load step
deal_log("Load phase Ended")

# Log that you have completed the ETL process
deal_log("ETL Job Ended")