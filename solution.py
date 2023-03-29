'''
    ***** Written by Chidiebere Onuoha******
    IMPORTANT:
        * Place the 'pryhon_task.zip' and the solution.py in the same folder before running the code.
        * Ensure ALL imported libraries are INSTALLED
        BEST WISHES :)
'''
import csv
import zipfile as zf
import os
import json
import pandas as pd

my_path = "files"
zipFile_path = "pryhon_task.zip"
jsonFile_path = "files/input.json"
outputCSV = "files/output.csv"  # Output CSV file path

def extractFiles(file_path: str, zip_path: str):
    '''
    :param file_path: This is the path to the created 'files' folder
    :param zip_path: This is the path to the zip file containing the CSV and JSON files
    :return: The return value is a message providing information whether the files folder exists or not.
    '''
    if os.path.exists(my_path) and os.path.isdir(my_path):
        return "Sample CSV and JSON files ALREADY extracted into 'files' folder!"
    else:
        msg = "The files folder does not exist!"
        with zf.ZipFile(zipFile_path, 'r') as files:
            # Extract all files into same directory
            files.extractall("files")
        return msg

def jsonToCSV(json_path):
    with open(json_path, 'r') as js:
        data = json.load(js)                                             # Reads JSON data into a list

    WorkContribution_data = []                                           # List contains dicts of strings
    productivityTotals_data = []                                         # List contains a dicts of jsons
    vrTotalEngineOperatingTime_data = []                                 # Contains vrTotalEngineOperatingTime data
    vrTotalMilledWeight_data = []                                        # Contains vrTotalMilledWeight data
    vrTotalMilledDistance_data = []                                      # Contains vrTotalMilledDistance data
    vrTotalMilledArea_data = []                                          # Contains vrTotalMilledArea data
    vrTotalMilledDifficultyDistance_data = []                            # Contains vrTotalMilledDifficultyDistance data
    vrTotalMilledRefinishArea_data = []
    vrTotalWaterConsumption_data = []
    vrTotalMilledVolume_data = []
    vrTotalJobDuration_data = []
    vrTotalMilledAdditionalArea_data = []
    vrTotalMilledDuration_data = []
    StartValues_data = []
    vrTotalJobDuration_data = []
    vrTotalWaterConsumption_data = []
    my_data = []

    # Extracting the productivityTotals data and appending to the corresponding list
    for i in range(len(data)):
        msg = data[i]["@message"]['message'].strip().split(', ')  # Extracts and splits string value
        WorkContribution_data.append(msg[1].split('='))
    for i in WorkContribution_data:
        productivityTotals_data.append({"WorkContribution": json.loads(i[1].strip(','))})

    '''
        * In this section, data is extracted according to their category:
            :category 1: 'productivityTotalStartValues'
            :category 2: 'productivityTotals'
    '''
    ptsv = 'productivityTotalStartValues'
    pt = 'productivityTotals'
    for i in productivityTotals_data:
        for k in range(0, 10):
            if i['WorkContribution'][ptsv][k]['value']['vrDomainId'] == "vrTotalMilledDifficultyDistance":
                vrTotalMilledDifficultyDistance_data.append(i['WorkContribution']['productivityTotalStartValues'][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalEngineOperatingTime":
                vrTotalEngineOperatingTime_data.append(i['WorkContribution']['productivityTotals'][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalMilledWeight":
                vrTotalMilledWeight_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalMilledDistance":
                vrTotalMilledDistance_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalMilledRefinishArea":
                vrTotalMilledRefinishArea_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalWaterConsumption":
                vrTotalWaterConsumption_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalMilledVolume":
                vrTotalMilledVolume_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalJobDuration":
                vrTotalJobDuration_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalMilledAdditionalArea":
                vrTotalMilledAdditionalArea_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalJobDuration":
                vrTotalJobDuration_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
            if i['WorkContribution'][pt][k]['value']['vrDomainId'] == "vrTotalWaterConsumption":
                vrTotalWaterConsumption_data.append(i['WorkContribution'][pt][k]['value']['valueAsDouble'])
        if i['WorkContribution'][pt][10]['value']['vrDomainId'] == "vrTotalMilledDuration":
            vrTotalMilledDuration_data.append(i['WorkContribution'][pt][10]['value']['valueAsDouble'])

    if len(vrTotalMilledDuration_data) < 1000:
        vrTotalMilledDuration_data.extend([0] * (1000 - len(vrTotalMilledDuration_data)))
    StartValues_data.extend(['X'] * 1000)

    for i in productivityTotals_data:
        vrTotalMilledArea_data.append(i['WorkContribution'][pt][3]['value']['valueAsDouble'])

    for i in data:
        date_time = i['@timestamp']
        session_id = i['mdc.SessionGuid']
        jobsite_id = i['@message']['mdc']['JobsiteId']
        machine_id = i['@message']['mdc']['MachineId']
        msg = i["@message"]['message'].strip().split(', ')  # Extracts and splits string value
        my_data.append({'Date/Time': date_time, 'SessionId': session_id, 'JobsiteId': jobsite_id, 'MachineId': machine_id})

    #print(f"x = {my_data[0:2]}, y = {productivityTotals_data[0:2]}")
    # Extracting the csv file name according to Pattern: %machineId_%startdate_%starttime_%endtime.csv
    m_id = my_data[0]['MachineId']
    date_id = my_data[0]['Date/Time'][:10].split('-')
    date_numbers = ''.join(filter(str.isdigit, ''.join(date_id)))
    startTime_id = my_data[0]['Date/Time'][11:16].split(':')
    startTime_numbers = ''.join([s for s in startTime_id if s.isdigit()])
    endTime_id = my_data[len(my_data)-1]['Date/Time'][11:16].split(':')
    endTime_numbers = ''.join([s for s in endTime_id if s.isdigit()])
    file_name = f"{m_id}_{date_numbers}_{startTime_numbers}_{endTime_numbers}_.csv"

    with open(file_name, mode='w', newline='') as file:
        # Define the fieldnames for the CSV file
        fieldnames = ['Date/Time', 'SessionId', 'JobsiteId', 'MachineId', 'vrTotalEngineOperatingTime',
                      'vrTotalMilledWeight', 'vrTotalMilledDistance', 'vrTotalMilledArea',
                      'vrTotalMilledDifficultyDistance','vrTotalMilledRefinishArea', 'vrTotalWaterConsumption',
                      'vrTotalMilledVolume','vrTotalJobDuration','vrTotalMilledAdditionalArea','vrTotalMilledDuration',
                      'StartValues','vrTotalJobDuration','vrTotalMilledDifficultyDistance','vrTotalMilledDuration',
                      'vrTotalWaterConsumption','vrTotalMilledWeight','vrTotalMilledRefinishArea','vrTotalMilledDistance',
                      'vrTotalMilledAdditionalArea','vrTotalEngineOperatingTime','vrTotalMilledVolume','vrTotalMilledArea']

        # Create a writer object and write the fieldnames to the first row
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        # Write the values of x and y to the CSV file
        for i, row in enumerate(my_data):   # This starts a for loop that iterates over each dictionary in x, where i is the index of the dictionary and row is the dictionary itself.
            row['vrTotalEngineOperatingTime'] = vrTotalEngineOperatingTime_data[i] # adds a new key-value pair to each dictionary row in x, where the key is 'vrTotalEngineOperatingTime' and the value is the corresponding element in the list y with the same index i.
            row['vrTotalMilledWeight'] = vrTotalMilledWeight_data[i]
            row['vrTotalMilledDistance'] = vrTotalMilledDistance_data[i]
            row['vrTotalMilledArea'] = vrTotalMilledArea_data[i]
            row['vrTotalMilledDifficultyDistance'] = vrTotalMilledDifficultyDistance_data[i]
            row['vrTotalMilledRefinishArea'] = vrTotalMilledRefinishArea_data[i-1]
            row['vrTotalMilledVolume'] = vrTotalMilledVolume_data[i]
            row['vrTotalJobDuration'] = vrTotalJobDuration_data[i-1]
            row['vrTotalMilledAdditionalArea'] = vrTotalMilledAdditionalArea_data[i]
            row['vrTotalMilledDuration'] = vrTotalMilledDuration_data[i]
            row['StartValues'] = StartValues_data[i]
            row['vrTotalJobDuration'] = vrTotalJobDuration_data[i]
            row['vrTotalMilledDifficultyDistance'] = vrTotalMilledDifficultyDistance_data[i]
            row['vrTotalMilledDuration'] = vrTotalMilledDuration_data[i]
            row['vrTotalWaterConsumption'] = vrTotalWaterConsumption_data[i]
            row['vrTotalMilledWeight'] = vrTotalMilledWeight_data[i]
            row['vrTotalMilledRefinishArea'] = vrTotalMilledRefinishArea_data[i - 1]
            row['vrTotalMilledDistance'] = vrTotalMilledDistance_data[i]
            row['vrTotalMilledAdditionalArea'] = vrTotalMilledAdditionalArea_data[i]
            row['vrTotalEngineOperatingTime'] = vrTotalEngineOperatingTime_data[i]
            row['vrTotalMilledVolume'] = vrTotalMilledVolume_data[i]
            row['vrTotalMilledArea'] = vrTotalMilledArea_data[i]
            writer.writerow(row)    # The writerow method writes the values of the dictionary to the file in the order of the keys. In this case, the keys are 'Date/Time', 'SessionId', 'JobsiteId', 'MachineId', and 'vrTotalEngineOperatingTime',

    return "Done!"

def main() -> None:
    extractFiles(my_path, zipFile_path)
    jsonToCSV(jsonFile_path)

if __name__ == "__main__":
    main()




