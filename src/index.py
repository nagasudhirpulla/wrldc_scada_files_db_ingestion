import datetime as dt
from fileHandler import FileHandler

# input variables
fileIds = ['demand']
startDt = dt.datetime(2018, 1, 1)
# startDt = dt.datetime.now() - dt.timedelta(days=1)
endDt = dt.datetime(2019, 12, 19)
# endDt = dt.datetime.now() - dt.timedelta(days=1)
fileHandler = FileHandler()

for fileId in fileIds:
    fileHandler.pushFileDataToDb(fileId, startDt, endDt)