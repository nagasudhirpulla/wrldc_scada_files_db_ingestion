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
                print('{0} returned only zero rows - {1}'.format(
                    targetDt.strftime('%d-%m-%Y'), dt.datetime.now().strftime('%H:%M:%S')))
                continue
            self.dataAdapter.connectToDb()
            self.dataAdapter.pushRows(dataRows)
            self.dataAdapter.disconnectDb()
            print('{0} data push with {1} rows done at {2}'.format(
                targetDt.strftime('%d-%m-%Y'), numRows, dt.datetime.now().strftime('%H:%M:%S')))
