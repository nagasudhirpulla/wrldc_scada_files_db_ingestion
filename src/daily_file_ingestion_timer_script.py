import datetime as dt
from fileHandler import FileHandler

# input variables
fileIds = ['volt', 'ict', 'gen_sch_act', 'sch_act_ui', 'state_gen']
# startDt = dt.datetime.now() - dt.timedelta(days=4)
startDt = dt.datetime.now() - dt.timedelta(days=1)
# endDt = dt.datetime(2019, 1, 10)
endDt = dt.datetime.now() - dt.timedelta(days=1)
fileHandler = FileHandler()

for fileId in fileIds:
    fileHandler.pushFileDataToDb(fileId, startDt, endDt)
