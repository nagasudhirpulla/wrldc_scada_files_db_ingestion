import pandas as pd
import datetime as dt
import os

targetDt = dt.datetime.now() - dt.timedelta(days=1)

fileMasterDf = pd.read_excel('master_data/scada_files_info_master.xlsx',
                             sheet_name='files')

fileInfoRow = fileMasterDf.iloc[0]
folderPath = fileInfoRow.folderPath
fileNamePrefix = fileInfoRow.prefix
fileDateFormat = fileInfoRow.dateFormat
fileType = fileInfoRow.fileType
fileNameSuffix = fileInfoRow.suffix
headerSkip = fileInfoRow.headerSkip
footerSkip = fileInfoRow.footerSkip
derivedFileName = fileNamePrefix + dt.datetime.strftime(targetDt, fileDateFormat) + fileNameSuffix
fullfilePath = os.path.join(folderPath, derivedFileName)
voltDf = None
if os.path.exists(fullfilePath):
    voltDf = pd.read_csv(fullfilePath, header=headerSkip, skipfooter=footerSkip)
