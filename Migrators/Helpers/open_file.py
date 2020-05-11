import csv 

def openFile(filePath, num):
	if num is not 0:
		with open(filePath) as csvfile:
			csvreader = csv.reader(csvfile, delimiter='	')
			raw_file = []
			n = 0
			for row in csvreader: 
				n = n + 1
				if n is not 1:
					raw_file.append(row)
		return raw_file