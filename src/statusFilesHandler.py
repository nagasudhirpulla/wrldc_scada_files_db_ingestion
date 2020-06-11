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
            statusStr = dataDf['status'].iloc[rowIter]
            dataRows.append(
                {
                    'data_time': dt.datetime.strptime(timeStr, '%d_%m_%Y_%H_%M_%S'),
                    'name': nameStr,
                    'ip': ipStr,
                    'status': str(statusStr)
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
        isSuccess = self.dataAdapter.pushRows(dataRows)
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
