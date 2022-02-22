#!/usr/bin/env python
# coding: utf-8

# once connected, call visplore.send_data(dataframe) to send your pandas dataframe to Visplore.
# See help(visplorepy) for details.

# libs
import pyodbc # connects to PdM database via ODBC interface
import pandas as pd 
import numpy as np
import yaml
import time 
from tkinter import * 
import visplorepy
from tkinter import ttk
from tkinter import scrolledtext
import os
import sys
from collections import Counter

## Global settings 
pd.set_option('display.max_columns', None) # show all columns, not just the first and last ones
datetimeFormat = '%Y-%m-%d %H:%M:%S' # define date format
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


# Parameters
with open(os.path.join(__location__, "XPress_Parameter.yaml")) as parFile:
    parDict = yaml.load(parFile, Loader=yaml.FullLoader)
parCompanyCode = parDict.get("CompanyCode") 
parWorkCenter = parDict.get("WorkCenter")
parBegin = parDict.get("Begin")
parEnd = parDict.get("End")
parOperation = parDict.get("Operation")
parMaterial = parDict.get("Material")
parReferenceCurve = parDict.get("ReferenceCurve")

log = None

def most_common(lst):
    data = Counter(lst)
    return max(lst, key=data.get)

def load_data(prodorder, material, begin, end, curveid):
    global log

    ## Connect DB
    log.insert(END,'Connecting to EHS_MT SQL DWH\n')
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=sasqlmibV02c\shared_prod1;DATABASE=EHS_MT;UID=EHS_MT_Reader;PWD=SQL4ehs&mt@miba!')

    ## Define Query
    query = "SELECT 0 as TimeRef ,[MCH_WorkCenter], [MCOD_OrderNumber], [MCOD_Operation],[MCOD_OperationState],[MCOD_ConfirmationNumber],[MCOD_MaterialNumber],[SysDateTimeOPC],[PDT_Force_LR],[PDT_Force_UR],[PDT_Position_1_2],[PDT_Position_LR],[PDT_Position_UR],[PDT_Gesamthubzaehler],[PDT_Hubzeit_aktuell],0 as PDT_Force_LR_Rerf,0 as PDT_Force_UR_Rerf,0 as PDT_Position_LR_Rerf,0 as PDT_Position_UR_Rerf FROM [IIOTMIBDB3010].[dbo].[IIOT_DataStorage_HF_XPress]"

    ## Load Process Data
    log.insert(END,"Loading PDT HF data from DWH for " + str(parWorkCenter) + "\n")
    query = query   + " WHERE convert(datetime2, SysDateTimeOPC, 101) > convert(datetime2, '" + begin + "', 104) AND convert(datetime2, SysDateTimeOPC, 101)  < convert(datetime2, '" + end + "', 104)"
    if prodorder != "":
        query = query  + " AND MCOD_OrderNumber ='" + prodorder + "'"
    if material != "":
        query = query  + " AND MCOD_MaterialNumber ='" + material + "'"
    start_time = time.time()
    df = pd.DataFrame()  
    try:
        df = pd.read_sql_query(query,cnxn)
    except Exception as e:
        log.insert(END,"Query invalid " + str(query) + "\n")
        return df
    end_time = time.time()
    time_elapsed = (end_time - start_time)
    numRows = df['TimeRef'].count()
    log.insert(END,"Query finished (Rows: " + f'{numRows:,}'.replace(',', '.') + " / Duration: " + str(round(time_elapsed,1)) + "sec)\n")
    if numRows == 0:
        return df

    ## Load Reference Curve
    if curveid != "":
        log.insert(END,"Loading reference curve\n")
        query = "SELECT 0 as TimeRef ,[MCH_WorkCenter], [MCOD_OrderNumber], [MCOD_Operation],[MCOD_OperationState],[MCOD_ConfirmationNumber],[MCOD_MaterialNumber],[SysDateTimeOPC], [PDT_Force_LR], [PDT_Force_UR],[PDT_Position_1_2],[PDT_Position_LR], [PDT_Position_UR], [PDT_Gesamthubzaehler], [PDT_Hubzeit_aktuell],0 as PDT_Force_LR_Rerf,0 as PDT_Force_UR_Rerf,0 as PDT_Position_LR_Rerf,0 as PDT_Position_UR_Rerf  FROM [IIOTMIBDB3010].[dbo].[IIOT_DataStorage_HF_XPress]"
        query = query   + " WHERE PDT_Gesamthubzaehler = '" + curveid + "'"
        start_time = time.time()
        dfRef = pd.read_sql_query(query,cnxn)
        end_time = time.time()
        time_elapsed = (end_time - start_time)
        numRows = dfRef['TimeRef'].count()
        log.insert(END,"Query finished (Rows: " + f'{numRows:,}'.replace(',', '.') + " / Duration: " + str(round(time_elapsed,1)) + "sec)\n")
    
    ## Format timestamp in defined format
    df['SysDateTimeOPC'] = pd.to_datetime(df['SysDateTimeOPC'], format=datetimeFormat)
    dfRef['SysDateTimeOPC'] = pd.to_datetime(dfRef['SysDateTimeOPC'], format=datetimeFormat)
    # Sort sata by their Timestamp
    df.sort_values(by=['SysDateTimeOPC'], inplace = True)
    dfRef.sort_values(by=['SysDateTimeOPC'], inplace = True)
    # Compute a numpy array out of dataframe for later calculations
    dfArray = df.to_numpy()
    dfArrayRef = dfRef.to_numpy()
    
    ## Create a blank matrix with just zeros and the new timestamps 
    log.insert(END,'Create matrix with 10ms sample rate\n')
    # Define start and end time stamp
    # Rebuild the dataframe with new indexes to get the first and last timestamp of data collection 
    dfHelp = pd.DataFrame(data=dfArray, columns=df.columns)
    startTime = dfHelp['SysDateTimeOPC'][0]
    endTime = dfHelp['SysDateTimeOPC'][len(dfHelp.index)-1]

    dfHelpRef = pd.DataFrame(data=dfArrayRef, columns=dfRef.columns)
    startTimeRef = dfHelpRef['SysDateTimeOPC'][0]
    endTimeRef = dfHelpRef['SysDateTimeOPC'][len(dfHelpRef.index)-1]

    # Define timeRange with frequency
    timeRange = pd.date_range(start = startTime, end = endTime, freq='0.01S')
    timeRangeRef= pd.date_range(start = startTimeRef, end = endTimeRef, freq='0.01S')


    # Define a column for the output of the  (number of ing-processes)
    colsAll = np.array(df.columns)
    colsAllRef = np.array(dfRef.columns)

    # Create a matrix with n = len(timeRange) Zeilen and len(columns_unified) columns and define the columns saved in colsAll as dataframe columns
    dfMainZero = pd.DataFrame(np.zeros((len(timeRange), len(df.columns))), columns = colsAll)
    dfMainZeroRef = pd.DataFrame(np.zeros((len(timeRangeRef), len(dfRef.columns))), columns = colsAllRef)

    # Set the Time time range
    dfMainZero['SysDateTimeOPC'] = timeRange
    dfMainZero['SysDateTimeOPC'] = pd.to_datetime(dfMainZero['SysDateTimeOPC'], format= datetimeFormat)
    dfMainZeroRef['SysDateTimeOPC'] = timeRangeRef
    dfMainZeroRef['SysDateTimeOPC'] = pd.to_datetime(dfMainZeroRef['SysDateTimeOPC'], format= datetimeFormat)

    # Make numpy array out of it for further proceeding
    dfMainZeroArray = dfMainZero.to_numpy()
    dfMainZeroArrayRef = dfMainZeroRef.to_numpy()


    
    # Artificial Time Stamp
    log.insert(END,'Create artifiial time stamp\n')
    starttime = dfMainZeroArray[0,7]
    i = 0
    while i < len(dfMainZeroArray):
        acttime = dfMainZeroArray[i,7]
        diffms = acttime - starttime
        dfMainZeroArray[i,0] = diffms.total_seconds() * 1000
        i  += 1
    
    starttime = dfMainZeroArrayRef[0,7]
    i=0
    while i < len(dfMainZeroArrayRef):
        acttime = dfMainZeroArrayRef[i,7]
        diffms = acttime - starttime
        dfMainZeroArrayRef[i,0] = diffms.total_seconds() * 1000
        i  += 1


    # RefCurve
    log.insert(END,'Sample Reference Curve\n')
    x = 0
    startIndex = 0
    endIndex = -1
    numParameters = 4
    while x < len(dfMainZeroArrayRef):
        y = startIndex
        while y < len(dfArrayRef):
            diff = dfMainZeroArrayRef[x,7] - dfArrayRef[y,7]
            if diff.days < 0:
                endIndex = y#-1
                if endIndex > startIndex:
                    hilfsMatrix = dfArrayRef[startIndex:endIndex, 0:8+numParameters+2]
                    z = 0
                    while z < numParameters:
                        dfMainZeroArrayRef[x,8+z] = float(np.mean(hilfsMatrix[:,8+z]))
                        z += 1
                    dfMainZeroArrayRef[x,1] = most_common(hilfsMatrix[:,1]) # MCH_WorkCenter
                    dfMainZeroArrayRef[x,2] = most_common(hilfsMatrix[:,2]) # MCOD_OrderNumber
                    dfMainZeroArrayRef[x,3] = most_common(hilfsMatrix[:,3]) # MCOD_Operation
                    dfMainZeroArrayRef[x,4] = most_common(hilfsMatrix[:,4]) # MCOD_OperationState
                    dfMainZeroArrayRef[x,5] = most_common(hilfsMatrix[:,5]) # MCOD_ConfirmationNumber
                    dfMainZeroArrayRef[x,6] = most_common(hilfsMatrix[:,6]) # MCOD_MaterialNumber
                    dfMainZeroArrayRef[x,12] = most_common(hilfsMatrix[:,12]) # MCOD_StrokeID
                    dfMainZeroArrayRef[x,13] = most_common(hilfsMatrix[:,13]) # MCOD_StrokeTime
                    x += 1
                    startIndex = endIndex# +1 
                    break
                else:      
                    x += 1
                    break
            y += 1
        if  y == len(dfArrayRef):
            x += 1


    log.insert(END,'Sample Process Data\n')
    x = 0
    startIndex = 0
    endIndex = -1
    numParameters = 4
    while x < len(dfMainZeroArray):
        y = startIndex
        while y < len(dfArray):
            diff = dfMainZeroArray[x,7] - dfArray[y,7]
            if diff.days < 0:
                endIndex = y#-1
                if endIndex > startIndex:
                    hilfsMatrix = dfArray[startIndex:endIndex, 0:14] # TimeStamp
                    dfMainZeroArray[x,1] = most_common(hilfsMatrix[:,1]) # MCH_WorkCenter
                    dfMainZeroArray[x,2] = most_common(hilfsMatrix[:,2]) # MCOD_OrderNumber
                    dfMainZeroArray[x,3] = most_common(hilfsMatrix[:,3]) # MCOD_Operation
                    dfMainZeroArray[x,4] = most_common(hilfsMatrix[:,4]) # MCOD_OperationState
                    dfMainZeroArray[x,5] = most_common(hilfsMatrix[:,5]) # MCOD_ConfirmationNumber
                    dfMainZeroArray[x,6] = most_common(hilfsMatrix[:,6]) # MCOD_MaterialNumber
                    dfMainZeroArray[x,12] = most_common(hilfsMatrix[:,12]) # MCOD_StrokeID
                    dfMainZeroArray[x,13] = most_common(hilfsMatrix[:,13]) # MCOD_StrokeTime
                    hilfsMatrix = dfArray[startIndex:endIndex, 8:8+numParameters]
                    z = 0
                    while z < numParameters:
                        dfMainZeroArray[x,8+z] = float(np.mean(hilfsMatrix[:,z]))
                        z += 1
                    x += 1
                    startIndex = endIndex# +1 
                    break
                else:
                    x += 1
                    break
            y += 1
        if  y == len(dfArray):
            x += 1


  
    # Merge Data
    log.insert(END,'Merge Data\n')
    previd = None
    i=0
    while i < len(dfMainZeroArray):
        actid = dfMainZeroArray[i,12]
        actts = dfMainZeroArray[i,13]
        if(actid > 0 and actid != previd):
            j = 0
            while i+j  < len(dfMainZeroArrayRef):
                dfMainZeroArray[i+j,14] = dfMainZeroArrayRef[j,8] 
                dfMainZeroArray[i+j,15] = dfMainZeroArrayRef[j,9] 
                dfMainZeroArray[i+j,16] = dfMainZeroArrayRef[j,10] 
                dfMainZeroArray[i+j,17] = dfMainZeroArrayRef[j,11] 
                j += 1
            previd = actid
        i  += 1
    

    
    # Dertermine if Ref Curve is outside selection and append it if necessary
    timeStampMIN = dfMainZeroArray[0,7]
    timeStampMAX = dfMainZeroArray[len(dfMainZeroArray)-1,7]
    timeStampRefMIN = dfMainZeroArrayRef[0,7]
    timeStampRefMAX = dfMainZeroArrayRef[len(dfMainZeroArrayRef)-1,7]
    if timeStampRefMIN >= timeStampMIN and timeStampRefMAX <= timeStampMAX:
        log.insert(END, 'Reference Curve located within selection\n')
        dfMainZeroArrayMerged = dfMainZeroArray
    else:
        log.insert(END, 'Reference Curve located outside selection => Append\n')
        lastTimeStamp = dfMainZeroArray[len(dfMainZeroArray)-1,0]
        sampleRate = lastTimeStamp - dfMainZeroArray[len(dfMainZeroArray)-2,0]
        startTime = lastTimeStamp + sampleRate
        log.insert(END, str(lastTimeStamp) + '\n')
        log.insert(END, str(sampleRate) + '\n')
        log.insert(END, str(startTime) + '\n')
        i=0
        while i < len(dfMainZeroArrayRef):
            dfMainZeroArrayRef[i,0] = startTime + sampleRate * i
            i  += 1
        dfMainZeroArrayMerged = np.concatenate((dfMainZeroArray, dfMainZeroArrayRef))


    ## Cleanse
    log.insert(END,'Cleanse Matrix\n')
    dfMainFinalDirty = pd.DataFrame(data=dfMainZeroArrayMerged,  columns=colsAll)
    isNotEmpty = (dfMainFinalDirty['PDT_Gesamthubzaehler'] != 0.0) #& (dfMainFinalDirty['PDT_Force_LR_Rerf'] != 0.0)
    dfMainFinal = dfMainFinalDirty[isNotEmpty]

    return dfMainFinal


def start_visplore(prodorder, material, begin, end, curveid):
    global log
    df = load_data(prodorder, material, begin, end, curveid)
    if df.empty:
        log.insert(END,'No Data Found\n')
        return
    visplore = visplorepy.start_visplore()
    visplore.send_data(df)
    visplore.start_cockpit("Trends and Distributions")


def export_csv(prodorder, material, begin, end, curveid):
    global log
    df = load_data(prodorder, material, begin, end, curveid)
    if df.empty:
        log.insert(END,'No Data Found\n')
        return
    log.insert(END,'Exporting CSV file\n')
    df.to_csv (os.path.join(__location__, "export.csv"), index = False, header=True, sep = ';')
    log.insert(END,'CSV file ist ready\n')
    file = os.path.join(__location__, "export.csv")
    os.startfile(file)


window = Tk()
window.title("Visplore Data Preperator")
#window.geometry("300x450")

Label(window, text="Production Order", width=20, anchor="e").grid(row=0)
Label(window, text="Material", width=20, anchor="e").grid(row=1)
Label(window, text="Begin", width=20, anchor="e").grid(row=2)
Label(window, text="End", width=20, anchor="e").grid(row=3)
Label(window, text="Reference Curve", width=20, anchor="e").grid(row=4)

i1 = StringVar(window)
i2 = StringVar(window)
i3 = StringVar(window)
i4 = StringVar(window)
i5 = StringVar(window)

i1.set(parDict.get("Operation"))
i2.set(parDict.get("Material"))
i3.set(parDict.get("Begin"))
i4.set(parDict.get("End"))
i5.set(parDict.get("ReferenceCurve"))

e1 = Entry(window, textvariable=i1)
e2 = Entry(window, textvariable=i2)
e3 = Entry(window, textvariable=i3)
e4 = Entry(window, textvariable=i4)
e5 = Entry(window, textvariable=i5)

e1.grid(row=0, column=1)
e2.grid(row=1, column=1)
e3.grid(row=2, column=1)
e4.grid(row=3, column=1)
e5.grid(row=4, column=1)

ttk.Separator(window,orient=HORIZONTAL).grid(row=5, columnspan=2, sticky="ew")

Button(window,
    text="Start Visplore",
    width=10,
    height=1,
    command=lambda: start_visplore(i1.get(), i2.get(), i3.get(), i4.get(), i5.get()),
).grid(row=6, column=0)

Button(window,
    text="Export CSV",
    width=10,
    height=1,
    command=lambda: export_csv(i1.get(), i2.get(), i3.get(), i4.get(), i5.get()),
).grid(row=6, column=1)




ttk.Separator(window,orient=HORIZONTAL).grid(row=7, columnspan=2, sticky="ew")

log = scrolledtext.ScrolledText(window, height=20, width=50)
log.grid(row=8, columnspan=2)


ttk.Separator(window,orient=HORIZONTAL).grid(row=9, columnspan=2, sticky="ew")


Button(window, 
    text="Quit",
    width=10,
    height=1,
    command=window.destroy
).grid(row=10, column=1)



log.insert(END,'Visplore Data preperation for XPress\n')
log.insert(END,'\n')
log.insert(END,'Parameters:\n')
log.insert(END,'===========\n')
#log.insert(END,'CompanyCode: ' + parCompanyCode + "\n")
#log.insert(END,'WorkCenter: ' + parWorkCenter + "\n")
log.insert(END,'Begin: ' + parBegin + "\n")
log.insert(END,'End: ' + parEnd + "\n")
log.insert(END,'Operation: ' + str(parOperation) + "\n")
log.insert(END,'Material: ' + str(parMaterial) + "\n")
log.insert(END,'ReferenceCurve: ' + str(parReferenceCurve) + "\n")
log.insert(END,'\n')

mainloop()

