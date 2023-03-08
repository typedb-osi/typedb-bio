import csv 

def readCSV(filePath, num_rows, skip_header = True, separator='\t'):
    if num_rows > 0:
        with open(filePath, encoding='utf8') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=separator)
            rows = []
            n = 0
            for row in csvreader:
                n = n + 1
                if n == 1 and skip_header:
                    continue
                else:
                    rows.append(row)
                if n == num_rows:
                    break
            return rows
    else:
        return []