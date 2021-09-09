import csv 

def openFile(filePath, num):
    if num != 0:
        with open(filePath) as csvfile:
            csvreader = csv.reader(csvfile, delimiter='\t')
            raw_file = []
            n = 0
            for row in csvreader:
                n = n + 1
                if n != 1:
                    raw_file.append(row)
        return raw_file