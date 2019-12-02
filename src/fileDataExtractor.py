import pandas as pd
import datetime as dt
from masterDataFetcher import MasterDataFetcher


class FileDataExtractor:
    masterDataFetcher = MasterDataFetcher()

    def getDbRowsForFile(self, fileId,
                         targetDt=dt.datetime.now() - dt.timedelta(days=1)):
        # output variable
        dataRows = []

        # get the the meta data of the required file Id
        fileInfoSeries = self.masterDataFetcher.getFileInfoSeries(fileId)

        # return empty array if meta data of file not found
        if not(isinstance(fileInfoSeries, pd.Series)):
            return []

        # get the data of the required file using the file metadata
        dataDf = self.masterDataFetcher.readFileDataDf(
            fileInfoSeries, targetDt)

        # return empty array if data file not found
        if not(isinstance(dataDf, pd.DataFrame)):
            return []

        # get the info of measurements to be extracted from the file
        fileMeasDf = self.masterDataFetcher.getFileMeasInfoDf(fileId)

        # extract timestamps from data DF
        timeColNum = fileInfoSeries.timeColumn
        timeFormat = fileInfoSeries.timeFormat
        timeVals = [dt.datetime.strptime(t, timeFormat)
                    for t in dataDf.iloc[:, timeColNum-1].values]

        # extract data for each measurement and append to array
        for measIter in range(fileMeasDf.shape[0]):
            measTag = fileMeasDf.iloc[measIter, :].tag
            columnNumber = fileMeasDf.iloc[measIter, :].columnNumber
            measDataVals = dataDf.iloc[:, columnNumber-1].values.tolist()
            for dataRowIter in range(len(measDataVals)):
                dataRow = {
                    'meas_time': timeVals[dataRowIter],
                    'meas_tag': measTag,
                    'meas_val': measDataVals[dataRowIter]
                }
                dataRows.append(dataRow)

        return dataRows
