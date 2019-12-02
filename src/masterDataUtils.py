import datetime as dt
import pandas as pd
import os

def getFileInfoSeries(fileId, fileMasterPath='master_data/scada_files_info_master.xlsx', fileMasterSheet='files'):
    fileMasterDf = pd.read_excel(fileMasterPath,
                                 sheet_name=fileMasterSheet)
    fileInfoSeries = fileMasterDf[fileMasterDf['Id'] == fileId].iloc[0]
    return fileInfoSeries

def readDataDf(fileInfoSeries, targetDt):
    folderPath = fileInfoSeries.folderPath
    fileNamePrefix = fileInfoSeries.prefix
    fileDateFormat = fileInfoSeries.dateFormat
    fileType = fileInfoSeries.fileType
    fileNameSuffix = fileInfoSeries.suffix
    fileType = fileInfoSeries.fileType
    headerSkip = fileInfoSeries.headerSkip
    footerSkip = fileInfoSeries.footerSkip
    derivedFileName = fileNamePrefix + \
        dt.datetime.strftime(targetDt, fileDateFormat) + fileNameSuffix
    fullfilePath = os.path.join(folderPath, derivedFileName)
    dataDf = None
    if os.path.exists(fullfilePath):
        if fileType == 'csv':
            dataDf = pd.read_csv(
                fullfilePath, header=headerSkip, skipfooter=footerSkip)
        elif fileType == 'xlsx':
            dataDf = pd.read_excel(
                fullfilePath, header=headerSkip, skipfooter=footerSkip)
    return dataDf

def getFileMeasInfoDf(fileId, measMasterPath='master_data/scada_files_info_master.xlsx', measMasterSheet='file_meas_info'):
    measMasterDf = pd.read_excel(measMasterPath,
                                 sheet_name=measMasterSheet)
    measInfoDf = measMasterDf[measMasterDf['fileId'] == fileId]
    return measInfoDf