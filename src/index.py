import datetime as dt
from fileHandler import FileHandler

# input variables
fileId = 'volt'
startDt = dt.datetime.now() - dt.timedelta(days=4)
endDt = dt.datetime.now() - dt.timedelta(days=3)
fileHandler = FileHandler()
fileHandler.pushFileDataToDb(fileId, startDt, endDt)