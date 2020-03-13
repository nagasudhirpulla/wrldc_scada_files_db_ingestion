import pandas as pd
import datetime as dt
from scadaDbAdapter import ScadaDbAdapter
import os
from glob import glob


class chunkFileHandler:
    dataAdapter = ScadaDbAdapter()

    def readDbRowsFromFile(self, filePath):
        dataRows = []
        if not(os.path.exists(filePath)):
            return dataRows

        dataDf = pd.read_csv(filePath, header=None)

        if dataDf.shape[1] != 3:
            return dataRows

        for rowIter in dataDf.shape[0]:
            idStr = dataDf.iloc[rowIter, 0]
            timeStr = dataDf.iloc[rowIter, 1]
            valStr = dataDf.iloc[rowIter, 2]
            dataRows.append(
                {
                    'meas_time': dt.datetime.strptime(timeStr[:-4], '%d_%m_%Y_%H_%M_%S'),
                    'meas_tag': idStr,
                    'meas_val': valStr
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
                # delete the file
                os.remove(filePath)
