import datetime as dt
import pandas as pd
import os


class MasterDataFetcher:
    fileMasterPath = 'master_data/scada_files_info_master.xlsx'
    fileMasterSheet = 'files'
    measMasterPath = 'master_data/scada_files_info_master.xlsx'
    measMasterSheet = 'file_meas_info'

    def getFileInfoSeries(self, fileId):
        fileMasterDf = pd.read_excel(self.fileMasterPath,
                                     sheet_name=self.fileMasterSheet)
        fileInfoSeries = fileMasterDf[fileMasterDf['Id'] == fileId]
        if fileInfoSeries.shape[0] == 0:
            return None
        else:
            fileInfoSeries = fileInfoSeries.iloc[0]
        return fileInfoSeries

    def readFileDataDf(self, fileInfoSeries, targetDt):
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

    def getFileMeasInfoDf(self, fileId):
        measMasterDf = pd.read_excel(self.measMasterPath,
                                     sheet_name=self.measMasterSheet)
        measInfoDf = measMasterDf[measMasterDf['fileId'] == fileId]
        return measInfoDf
