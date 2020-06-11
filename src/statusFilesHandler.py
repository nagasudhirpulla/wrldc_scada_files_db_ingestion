import pandas as pd
import datetime as dt
from nodeStatusDbAdapter import NodeStatusDbAdapter
import os
from glob import glob


class StatusFilesHandler:
    dataAdapter = NodeStatusDbAdapter()

    def readDbRowsFromFile(self, filePath):
        dataRows = []
        if not(os.path.exists(filePath)):
            return dataRows

        dataDf = pd.read_csv(filePath)

        if dataDf.shape[0] == 0 or not(set(['data_time', 'ip', 'name', 'status']).issubset(set(dataDf.columns.tolist()))):
            return dataRows

        for rowIter in range(dataDf.shape[0]):
            timeStr = dataDf['data_time'].iloc[rowIter]
            ipStr = dataDf['ip'].iloc[rowIter]
            nameStr = dataDf['name'].iloc[rowIter]
            statusVal = dataDf['status'].iloc[rowIter]
            dataRows.append(
                {
                    'data_time': dt.datetime.strptime(timeStr, '%d_%m_%Y_%H_%M_%S'),
                    'name': nameStr,
                    'ip': ipStr,
                    'status': statusVal
                })
        return dataRows

    def pushFileDataToDb(self, filePath):
        # read file lines
        dataRows = self.readDbRowsFromFile(filePath)
        numRows = len(dataRows)
        if numRows == 0:
            print('{0} returned only zero rows - {1}'.format(
                filePath, dt.datetime.now().strftime('%H:%M:%S')))
            return False
        self.dataAdapter.connectToDb()
        # push node status rows to real time db
        isSuccess = self.dataAdapter.pushRows(dataRows)
        # get the diff of live nodes status and the latest hist data
        latestHistStatusDf = self.dataAdapter.fetchLatestNodeHistory()
        diffHistRows = self.getDiffNodeStatusRows(latestHistStatusDf, dataRows)
        # push hist rows to db
        self.dataAdapter.pushHistRows(diffHistRows)
        self.dataAdapter.disconnectDb()
        print('{0} data push with {1} rows done at {2}'.format(
            filePath, numRows, dt.datetime.now().strftime('%H:%M:%S')))
        return isSuccess

    def pushFolderFilesToDb(self, folderPath):
        if not(os.path.isdir(folderPath)):
            return
        fNames = glob(os.path.join(folderPath, '*.csv'))
        fNames.sort(key=os.path.getmtime)
        for filePath in fNames:
            print(filePath)
            isSuccess = self.pushFileDataToDb(filePath)
            if isSuccess == True:
                # delete the file after processing
                os.remove(filePath)

    def getDiffNodeStatusRows(self, dbStatus, newRows):
        # attributes of newRows objects = ip, status, name, data_time
        # column names of dbStatus dataFrame = 'name', 'data_time', 'status'
        diffRows = []
        for nRow in newRows:
            # get the dbStatus row with the same name as nRow but a different status
            nodeName = nRow['name']
            filteredDf = dbStatus[(dbStatus['name'] == nodeName) & (
                dbStatus['status'] == nRow['status'])]
            if filteredDf.shape[0] == 0:
                diffRows.append(nRow)
        return diffRows
