import pandas as pd
import datetime as dt
from src.nodeStatusDbAdapter import NodeStatusDbAdapter
import os
from glob import glob
from io import StringIO


class StatusFilesHandler:
    dataAdapter = NodeStatusDbAdapter()

    def readDbRowsFromDf(self, dataDf):
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

    def pushTextDataToDb(self, txt):
        dataRows = []
        try:
            dataDf = pd.read_csv(StringIO(txt))
            dataRows = self.readDbRowsFromDf(dataDf)
        except:
            dataRows = []
        self.pushDataRowsToDb(dataRows)

    def pushFileDataToDb(self, filePath):
        dataRows = []
        if not(os.path.exists(filePath)):
            return dataRows
        try:
            dataDf = pd.read_csv(filePath)
            dataRows = self.readDbRowsFromDf(dataDf)
        except:
            dataRows = []
        print("pushing data of file {0}".format(filePath))
        self.pushDataRowsToDb(dataRows)

    def pushDataRowsToDb(self, dataRows):
        # read file lines
        numRows = len(dataRows)
        if numRows == 0:
            print('{0} returned only zero rows - {1}'.format(
                filePath, dt.datetime.now().strftime('%H:%M:%S')))
            return False
        self.dataAdapter.connectToDb()

        # get the diff of live nodes status and the latest hist data
        liveNodeStatusDf = self.dataAdapter.fetchLiveNodeStatus()
        (liveDiffRows, histDiffRows) = self.getDiffNodeStatusRows(
            liveNodeStatusDf, dataRows)

        # push node status rows to real time db
        isSuccess = self.dataAdapter.pushRows(liveDiffRows)

        # push hist rows to db
        isSuccess = self.dataAdapter.pushHistRows(histDiffRows)
        self.dataAdapter.disconnectDb()
        print('data push with {0} rows done at {1}'.format(
            numRows, dt.datetime.now().strftime('%H:%M:%S')))
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

    def getDiffNodeStatusRows(self, liveNodeStatusDf, newRows):
        if len(newRows) == 0:
            return ([], [])
        # attributes of newRows objects = ip, status, name, data_time
        # column names of live node status dataFrame = name, status, data_time, last_toggled_at

        # create new df from incoming rows
        incomingDf = pd.DataFrame(newRows)

        joinedDf = incomingDf.merge(
            liveNodeStatusDf, on='name', how='left', suffixes=('', '_2'))
        # initialize the result lists
        liveDiffRows = []
        histDiffRows = []

        for rowIter in range(joinedDf.shape[0]):
            nodeName = joinedDf['name'].iloc[rowIter]
            lastToggledAt = joinedDf['last_toggled_at'].iloc[rowIter]
            incomingTime = joinedDf['data_time'].iloc[rowIter]
            lastToggledNew = incomingTime if pd.isnull(
                lastToggledAt) else lastToggledAt
            liveStatus = joinedDf['status_2'].iloc[rowIter]
            incomingStatus = joinedDf['status'].iloc[rowIter]
            nodeIp = joinedDf['ip'].iloc[rowIter]
            if pd.isnull(liveStatus) or (liveStatus != incomingStatus):
                lastToggledNew = incomingTime
                histDiffRows.append(
                    {'name': nodeName, 'status': incomingStatus, 'data_time': lastToggledNew})
            liveDiffRows.append({'name': nodeName, 'status': incomingStatus,
                                 'data_time': incomingTime, 'last_toggled_at': lastToggledNew, 'ip': nodeIp})
        return (liveDiffRows, histDiffRows)
