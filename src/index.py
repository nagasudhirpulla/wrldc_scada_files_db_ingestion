import datetime as dt
from fileHandler import FileHandler

# input variables
fileId = 'ict'
# startDt = dt.datetime.now() - dt.timedelta(days=4)
startDt = dt.datetime(2019, 1, 1)
# endDt = dt.datetime(2019, 1, 10)
endDt = dt.datetime.now() - dt.timedelta(days=1)
fileHandler = FileHandler()
fileHandler.pushFileDataToDb(fileId, startDt, endDt)
