from chunksHandler import ChunkFilesHandler
import sys

# resolve the chunks folder
chunksFolder = ''
for argIter in range(len(sys.argv)-1):
    argFlag = sys.argv[argIter]
    print(argFlag)
    if argFlag == "--chunksFolder":
        chunksFolder = sys.argv[argIter+1]

handler = ChunkFilesHandler()
handler.pushFolderFilesToDb(chunksFolder)