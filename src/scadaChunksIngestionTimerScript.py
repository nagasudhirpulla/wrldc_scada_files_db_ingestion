from chunksHandler import ChunkFilesHandler
import sys

# resolve the chunks folder
chunksFolder = ''
for argIter in range(len(sys.argv)-1):
    argFlag = sys.argv[argIter]
    # print(argFlag)
    if argFlag == "--chunksFolder":
        chunksFolder = sys.argv[argIter+1]

print('Ingesting files from folder - {0}'.format(chunksFolder))
handler = ChunkFilesHandler()
handler.pushFolderFilesToDb(chunksFolder)