
# IMPORTS

import re
import csv
import sys
import operator
from operator import itemgetter

#VARIABLES 
metadata = {} #Contains whole metadata
MetaDataFile = "./metadata.txt"
finalTable = []

#COLLECTING METADATA
def getMetadata(file):
	try:
		with open(file) as fileData:	
			tableStart = 0
			tableName = ""
			attributes = []
			for line in fileData.readlines():
				line = line.strip()				
				if(line == "<begin_table>"):
					tableStart = 1
					tableName = ""
					attributes = []
					continue
				if(tableStart == 1):
					tableName = line.lower()
					tableStart = 0			
					continue
				if(line == "<end_table>"):
					metadata[tableName] = attributes
					continue
				attributes.append(line.lower())
	except:
		print("Error: File Missing \nMake sure 'metadata.txt' is in the current repo.")
		sys.exit()

#CREATION OF A TABLE
def getTable(tableName):
	tempTable = []
	tableFile = tableName+".csv"
	with open(tableFile) as tableData:
		for line in tableData:
			line = line.replace("'","")
			line = line.replace('"','')
			line = line.replace(' ','')
			line = line.strip()
			line = line.split(",")
			colValues = [int(val) for val in line]
			tempTable.append(colValues)
		return tempTable

#CREATION OF A TABLE USING MULTIPLE TABLES
def getTables(tablesArray):
	tempTable1=[]
	for table in (tablesArray):
		tableName = table.strip()
		if(tableName not in metadata):
			print("Error: Table "+str(tableName)+" doesn't exists in "+str(MetaDataFile))
			sys.exit()
		if(len(tempTable1)==0):
			tempTable1 = getTable(tableName)
		else:
			tempTable = []
			tempTable2 = getTable(tableName)
			for i in (tempTable1):
				for j in (tempTable2):
					tempTable.append(i+j)
			tempTable1 = tempTable.copy()

	return tempTable1

#ASSIGNING INDEX FOR COLUMNS
def assignIndex(tablesArray):
	indexes = {}
	count = 0
	for table in tablesArray:
		table = table.strip()
		for col in metadata[table]:
			indexes[col] = count
			count+=1
	return indexes

#MAX QUERY OPERATION
def maxOutput(colIdx,table):
	maxValue = table[0][colIdx]
	for row in table:
		if(row[colIdx]>maxValue):
			maxValue = row[colIdx]
	return maxValue

#MIN QUERY OPERATION
def minOutput(colIdx,table):
	minValue = table[0][colIdx]
	for row in table:
		if(row[colIdx]<minValue):
			minValue = row[colIdx]
	return minValue

#SUM QUERY OPERATION
def sumOutput(colIdx,table):
	sumValue = 0
	for row in table:
		sumValue += row[colIdx]
	return sumValue

#AVG QUERY OPERATION
def avgOutput(colIdx,table):
	sumValue = 0
	for row in table:
		sumValue += row[colIdx]
	return (sumValue/len(table))

#COUNT QUERY OPERATION
def countOutput(colIdx,table):
	cntValue = 0
	for row in table:
		cntValue += 1
	return cntValue

#DISTINCT FUNCTION
def distinctTrue(table):
	output = []
	for row in table:
		if(row not in output):
			output.append(row)
	return output

#INT DETERMINER
def RepresentsInt(s):
	try: 
		float(s)
		return True
	except ValueError:
		return False

#WHERE QUERY APPLIER
def whereQueryOutput(whereQuery,table,idx):
	operations = {"!=": operator.ne, "=": operator.eq, ">=": operator.ge,
		   ">": operator.gt, "<": operator.lt, "<=": operator.le}

	oper = None
	outputVector = []
	if '!=' in whereQuery:
		oper = '!='
	elif '>=' in whereQuery:
		oper = '>='
	elif '>' in whereQuery:
		oper = '>'
	elif '<=' in whereQuery:
		oper = '<='
	elif '<' in whereQuery:
		oper = '<'
	elif '=' in whereQuery:
		oper = '='
	else:
		print("Error: Invalid Operator Used")
		sys.exit()
	subQuery1 = whereQuery.split(oper)[0].strip()
	subQuery2 = whereQuery.split(oper)[1].strip()
	for row in table:
		val1 = None
		val2 = None
		if(RepresentsInt(subQuery1)):
			val1 = int(subQuery1)
		else:
			val1 = row[idx[subQuery1.strip()]]

		if(RepresentsInt(subQuery2)):
			val2 = int(subQuery2)
		else:
			val2 = row[idx[subQuery2.strip()]]

		if(operations[oper](val1,val2)):
			outputVector.append(True)
		else:
			outputVector.append(False)

	return outputVector		


#TRANSPOSE FUNCTION
def transpose(l1):
	l2 = [] 
	l2 = list(map(list, zip(*l1))) 
	return l2 

#PRINTING FINAL DATA
def printData(table,Headers):
	for iterator in range(len(Headers)):
		row = Headers[iterator]
		query = row.split(":")
		queryCol = query[1]
		query = query[0]
		for tableName,colName in metadata.items():  
			if(query == "col"):
				if(iterator==len(Headers)-1):
					if queryCol in colName:
						print(tableName+"."+queryCol,end="")
				else:		
					if queryCol in colName:
						print(tableName+"."+queryCol,end=",")
			else:
				if(iterator==len(Headers)-1):
					if queryCol in colName:
						print(query+"("+tableName+"."+queryCol+")",end="")
				else:		
					if queryCol in colName:
						print(query+"("+tableName+"."+queryCol+")",end=",")

	print()
	for row in table:
		for idx in range(len(row)):
			if(idx==len(row)-1):
				print(row[idx],end="")
			else:	
				print(row[idx],end=",")
		print()
		
	print(table)
#PRINTING THE OUTPUT
def printOutput(columns,tablesArray,finalTable,aggregateQueries,distinctFlag,queryWhere,ANDFlag,ORFlag,orderByWay,orderByColumn,queryGroupByColumns,Headers):
	indices = []

	#printing Header
	columnsWithTables = []
	if(len(columns)!=0):	
		for col in (columns):
			col = col.strip()
			for table in (metadata):
				if (col in metadata[table]):
					columnsWithTables.append(str(str(table)+str(col)))
	indexes = assignIndex(tablesArray)

	#WhereCondition
	if(queryWhere!=None):
		whereQueryResult = []
		if(ANDFlag==True):
			subQuery1 = queryWhere.split('and')[0].strip()
			subQuery2 = queryWhere.split('and')[1].strip()
			subQuery1OP = whereQueryOutput(subQuery1,finalTable,indexes)
			subQuery2OP = whereQueryOutput(subQuery2,finalTable,indexes)
			for idx in range(len(subQuery1OP)):
				if(subQuery1OP[idx] and subQuery2OP[idx]):
					whereQueryResult.append(True)
				else:
					whereQueryResult.append(False)

		elif(ORFlag==True):
			subQuery1 = queryWhere.split('or')[0].strip()
			subQuery2 = queryWhere.split('or')[1].strip()
			subQuery1OP = whereQueryOutput(subQuery1,finalTable,indexes)
			subQuery2OP = whereQueryOutput(subQuery2,finalTable,indexes)
			for idx in range(len(subQuery1OP)):
				if(subQuery1OP[idx] or subQuery2OP[idx]):
					whereQueryResult.append(True)
				else:
					whereQueryResult.append(False)
		else:
			whereQueryResult = 	whereQueryOutput(queryWhere,finalTable,indexes)
		
		whereQueryUpdatedTable = []
		for idx in range(len(finalTable)):
			if(whereQueryResult[idx]):
				whereQueryUpdatedTable.append(finalTable[idx])

		finalTable = whereQueryUpdatedTable

	#GroupBy condition
	if(len(queryGroupByColumns)!=0):
		groupByFinalTable = []
		onlyGroupByFinalTableColumns = []
		for row in finalTable:
			colAsPerGroupBy = []
			for attr in queryGroupByColumns:
				colAsPerGroupBy.append(row[indexes[attr]])
			if(colAsPerGroupBy not in groupByFinalTable):
				groupByFinalTable.append(colAsPerGroupBy.copy())
				onlyGroupByFinalTableColumns.append(colAsPerGroupBy.copy())

		#defining Indexes as per new table after groupBy operation
		newIndexes = []
		newIndexesDict = {}
		count = 0
		for col in queryGroupByColumns:
			newIndexes.append([col,count])
			newIndexesDict[col] = count
			count+=1
		for col in indexes:
			if(col not in queryGroupByColumns):
				newIndexes.append([col,count])
				newIndexesDict[col] = count
				count+=1

		for row in groupByFinalTable:
			for iterator in range(len(queryGroupByColumns),len(indexes)):
				row.append([])

		for row in (finalTable):
			temp = []
			for attr in queryGroupByColumns:
				temp.append(row[indexes[attr]])

			reqIndex = onlyGroupByFinalTableColumns.index(temp)
			for iterator in range(len(queryGroupByColumns),len(indexes)):
				groupByFinalTable[reqIndex][newIndexes[iterator][1]].append(row[indexes[newIndexes[iterator][0]]])
		
		# print(groupByFinalTable)
		printingTable = []
		for row in groupByFinalTable:
			printingTable.append([])

		for query in aggregateQueries:
			query = query.split(":")
			col = query[1]
			query = query[0]
			colNumber = newIndexesDict[col]
			
			if(query=="max"):
				for row in range(len(groupByFinalTable)):
					if(isinstance(groupByFinalTable[row][colNumber],list)):
						printingTable[row].append(max(groupByFinalTable[row][colNumber]))
					else:
						printingTable[row].append(groupByFinalTable[row][colNumber])

			if(query=="min"):
				for row in range(len(groupByFinalTable)):
					if(isinstance(groupByFinalTable[row][colNumber],list)):
						printingTable[row].append(min(groupByFinalTable[row][colNumber]))
					else:
						printingTable[row].append(groupByFinalTable[row][colNumber])

			if(query=="avg"):
				for row in range(len(groupByFinalTable)):
					if(isinstance(groupByFinalTable[row][colNumber],list)):
						printingTable[row].append(sum(groupByFinalTable[row][colNumber])/len(sum(groupByFinalTable[row][colNumber])))
					else:
						printingTable[row].append(float(groupByFinalTable[row][colNumber]))

			if(query=="sum"):
				for row in range(len(groupByFinalTable)):
					if(isinstance(groupByFinalTable[row][colNumber],list)):
						printingTable[row].append(sum(groupByFinalTable[row][colNumber]))
					else:
						printingTable[row].append((groupByFinalTable[row][colNumber])*len((groupByFinalTable[row][len(groupByFinalTable[row])-1])))

			if(query=="count"):
				for row in range(len(groupByFinalTable)):
					if(isinstance(groupByFinalTable[row][colNumber],list)):
						printingTable[row].append(len(groupByFinalTable[row][colNumber]))
					else:
						printingTable[row].append(len((groupByFinalTable[row][len(groupByFinalTable[row])-1])))

			if(query=="col"):
				for row in range(len(groupByFinalTable)):
					printingTable[row].append(groupByFinalTable[row][colNumber])

		if(orderByWay!=None):
			printingTable.sort(key=itemgetter(newIndexesDict[orderByColumn]))
			if(orderByWay=="desc"):
				printingTable = printingTable[::-1]	

		printData(printingTable,Headers)
		sys.exit()	


	#OrderBy condition
	if(orderByWay!=None):
		finalTable.sort(key=itemgetter(indexes[orderByColumn]))
		if(orderByWay=="desc"):
			finalTable = finalTable[::-1]
				
	#printing aggregate queries outputs
	aggrHeaders = []
	aggrValues = []
	printingTable = []

	for query in aggregateQueries:
		aggregateUsed = 0
		query = query.split(":")
		col = query[1]
		query = query[0]
		if(query=="max"):
			aggregateUsed = 1
			val = maxOutput(int(indexes[col]),finalTable)
			printingTable.append(val)

		if(query=="min"):
			aggregateUsed = 1
			val = minOutput(int(indexes[col]),finalTable)
			printingTable.append(val)

		if(query=="avg"):
			aggregateUsed = 1
			val = avgOutput(int(indexes[col]),finalTable)
			aggrValues.append(val)
			printingTable.append(val)

		if(query=="sum"):
			aggregateUsed = 1
			val = sumOutput(int(indexes[col]),finalTable)
			printingTable.append(val)

		if(query=="count"):
			aggregateUsed = 1
			val = countOutput(int(indexes[col]),finalTable)
			printingTable.append(val)

	if(aggregateUsed == 1):
		printingTable = [printingTable]

	#getting printing table
	if(len(columns)!=0):
		printingTable = []
		for row in finalTable:
			printingRowVals = []
			for col in (columns):
				col = col.strip()
				printingRowVals.append(row[int(indexes[col])])
			printingTable.append(printingRowVals)
				
		if(distinctFlag == True):
			printingTable = distinctTrue(printingTable)

	printData(printingTable,Headers)

#REMOVING SPACES FUNCTION
def useSplit(array):
	for idx in range(len(array)):
		array[idx] = array[idx].split()[0]
	return array

#QUERY VALIDATION
def queryValidator(query):
	validationOutput = bool(re.match('^select.*from.*', query))
	return validationOutput

#QUERY PROCESSING
def queryProcessor(query):
	# originalQuery = (query)
	query = query.lower()
	if(queryValidator(query)==False):
		print("Entered Query is of Incorrect format. \nCorrect format of Query is: Select <columns> from <tables> where <condition>")
		sys.exit()

	distinctFlag = False	
	ANDFlag = False
	ORFlag = False
	CountStarFlag = False
	if(bool(re.match('.*count\(.*\*.*\).*', query))):
		CountStarFlag = True
		query.replace("count\(.*\*.*\)","count(StarFlag)")
	if(query.count('*')>1):
		print("Error: There is no column named *")
		sys.exit()

	queryWithoutSelect = query.replace("select","").strip()

	queryColumns = queryWithoutSelect.split("from")[0].strip()
	if bool(re.match('^distinct.*', queryColumns)):
		distinctFlag = True
		queryColumns = queryColumns.replace('distinct', '').strip()

	queryColumns = queryColumns.split(",")
	queryColumns = useSplit(queryColumns)

	aggregateQueries = []
	onlyAggrQueries = []
	queriesWithoutAggrQueries = queryColumns.copy()
	for idx in range(len(queryColumns)):
		col = queryColumns[idx]
		col = col.strip()
		temp = ""
		if bool(re.match('^(max)\(.*\)', col)):
			temp = col.replace('max', '').strip().strip('()')
			colNames = temp.split(',')
			if len(colNames) > 1:
				print("Error: More than one column is used with aggregate functions \nOnly one column is allowed with aggregate functions")
				sys.exit()
			aggregateQueries.append("max:"+temp)
			onlyAggrQueries.append(col)
			queriesWithoutAggrQueries.remove(col)
			continue

		elif bool(re.match('^(min)\(.*\)', col)):
			temp = col.replace('min', '').strip().strip('()')
			colNames = temp.split(',')
			if len(colNames) > 1:
				print("Error: More than one column is used with aggregate functions \nOnly one column is allowed with aggregate functions")
				sys.exit()
			aggregateQueries.append("min:"+temp)
			onlyAggrQueries.append(col)
			queriesWithoutAggrQueries.remove(col)
			continue

		elif bool(re.match('^(avg)\(.*\)', col)):
			temp = col.replace('avg', '').strip().strip('()')
			colNames = temp.split(',')
			if len(colNames) > 1:
				print("Error: More than one column is used with aggregate functions \nOnly one column is allowed with aggregate functions")
				sys.exit()
			aggregateQueries.append("avg:"+temp)
			onlyAggrQueries.append(col)
			queriesWithoutAggrQueries.remove(col)
			continue

		elif bool(re.match('^(sum)\(.*\)', col)):
			temp = col.replace('sum', '').strip().strip('()')
			colNames = temp.split(',')
			if len(colNames) > 1:
				print("Error: More than one column is used with aggregate functions \nOnly one column is allowed with aggregate functions")
				sys.exit()
			aggregateQueries.append("sum:"+temp)
			onlyAggrQueries.append(col)
			queriesWithoutAggrQueries.remove(col)
			continue

		elif bool(re.match('^(count)\(.*\)', col)):
			temp = col.replace('count', '').strip().strip('()')
			colNames = temp.split(',')
			if len(colNames) > 1:
				print("Error: More than one column is used with aggregate functions \nOnly one column is allowed with aggregate functions")
				sys.exit()
			aggregateQueries.append("cnt:"+temp)
			onlyAggrQueries.append(col)
			queriesWithoutAggrQueries.remove(col)
			continue
		aggregateQueries.append("col:"+col)
	Headers = aggregateQueries.copy()


	queryTables = queryWithoutSelect.split("from")[1].strip()
	queryTables = queryTables.split("where")[0].strip()
	queryTables = queryTables.split("group")[0].strip()
	queryTables = queryTables.split("order")[0].strip()
	queryTables = queryTables.split(",")
	queryTables = useSplit(queryTables)


	# allColumnsString = ""

	# for idx in queryTables:
	# 	for iterator in metadata[idx]:
	# 		allColumnsString = allColumnsString+str(iterator)+" "

	# queryWithoutSelect = queryWithoutSelect.replace('*',allColumnsString)

	queryWhere = None
	if(bool(re.match('.*where.*', queryWithoutSelect))):
		queryWhere = queryWithoutSelect.split("where")[1].strip()
		queryWhere = queryWhere.split("order")[0].strip()
		queryWhere = queryWhere.split("group")[0].strip()
		if(bool(re.match('.*and.*', queryWhere))):
			ANDFlag = True
		elif(bool(re.match('.*or.*', queryWhere))):
			ORFlag = True
	
	queryGroupBy = None
	queryGroupByColumns = []
	if(bool(re.match('.*group by.*', queryWithoutSelect))):
		queryGroupBy = queryWithoutSelect.split("group by")[1].strip()
		queryGroupBy = queryGroupBy.split("order by")[0].strip()
		queryGroupByColumns = queryGroupBy.split(",")

	queryOrderWay = None
	queryOrderColumn = None
	if(bool(re.match('.*order by.*', queryWithoutSelect))):
		queryOrderWay = "asc"
		queryOrderColumn = None
		queryOrder = queryWithoutSelect.split("order by")[1].strip()
		queryOrder = queryOrder.split("group")[0].strip()
		queryOrder = queryOrder.split("where")[0].strip()
		if(bool(re.match('.*asc.*', queryOrder))):
			queryOrderWay = "asc"
			queryOrder = queryOrder.replace("asc","").strip()
		if(bool(re.match('.*desc.*', queryOrder))):
			queryOrderWay = "desc"
			queryOrder = queryOrder.replace("desc","").strip()
		queryOrderColumn = queryOrder.strip()

	finalTable = getTables(queryTables)
	printOutput(queriesWithoutAggrQueries,queryTables,finalTable,aggregateQueries,distinctFlag,queryWhere,ANDFlag,ORFlag,queryOrderWay,queryOrderColumn,queryGroupByColumns,Headers)

#MAIN FUNCTION
def main():
	getMetadata(MetaDataFile)
	query = sys.argv[1]
	if(query[-1]!=";"):
		print("Ending SemiColon Missing")
		sys.exit()

	query = query[:-1].strip()
	queryProcessor(query)	

#MAIN CODE
if __name__ == '__main__':
	main()