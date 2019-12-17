import pandas as pd
import datetime as dt
from fileDataExtractor import FileDataExtractor
from scadaDbAdapter import ScadaDbAdapter


class FileHandler:
    # extract data from file
    dataExtractor = FileDataExtractor()
    # push file data to db
    dataAdapter = ScadaDbAdapter()

    def pushFileDataToDb(self, fileId, startDate, endDate):
        for dayOffset in range((endDate-startDate).days+1):
            targetDt = startDate+dt.timedelta(days=dayOffset)
            dataRows = self.dataExtractor.getDbRowsForFile(fileId, targetDt)
            numRows = len(dataRows)
            if numRows == 0:
                print('{0} {1} returned only zero rows - {2}'.format(
                    targetDt.strftime('%d-%m-%Y'), fileId, dt.datetime.now().strftime('%H:%M:%S')))
                continue
            self.dataAdapter.connectToDb()
            self.dataAdapter.pushRows(dataRows)
            self.dataAdapter.disconnectDb()
            print('{0} {1} data push with {2} rows done at {3}'.format(
                targetDt.strftime('%d-%m-%Y'), fileId, numRows, dt.datetime.now().strftime('%H:%M:%S')))