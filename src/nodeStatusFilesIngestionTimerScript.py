from statusFilesHandler import StatusFilesHandler
import sys

# resolve the ip nodes status files folder
filesFolder = ''
for argIter in range(len(sys.argv)-1):
    argFlag = sys.argv[argIter]
    # print(argFlag)
    if argFlag == "--filesFolder":
        filesFolder = sys.argv[argIter+1]

print('Ingesting ip nodes status files from folder - {0}'.format(filesFolder))
handler = StatusFilesHandler()
handler.pushFolderFilesToDb(filesFolder)
