import os

dir = "E:/me"
fileList = os.listdir(dir)
for fileName in fileList :
    if fileName.startswith("readme"):
        filePath = os.path.join("%s/%s" % (dir, fileName))

        with open(filePath, encoding="utf8") as f:
            for fLine in f:
                print(fLine, end="")