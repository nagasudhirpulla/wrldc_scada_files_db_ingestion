import pandas as pd
import datetime as dt
from masterDataUtils import readDataDf, getFileInfoSeries, getFileMeasInfoDf

# get the the meta data of the required file Id
fileId = 'volt'
fileMasterPath = 'master_data/scada_files_info_master.xlsx'
fileMasterSheet = 'files'

fileInfoSeries = getFileInfoSeries(fileId, fileMasterPath, fileMasterSheet)

targetDt = dt.datetime.now() - dt.timedelta(days=1)

# get the data of the required file using the file metadata
dataDf = readDataDf(fileInfoSeries, targetDt)

# get the measurements to be extracted from the file
measMasterPath = fileMasterPath
measMasterSheet = 'file_meas_info'
fileMeasDf = getFileMeasInfoDf(fileId, measMasterPath, measMasterSheet)
