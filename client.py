# sampleURL
# userId is a five digit number
# resourceId is a number less than 100000
# operator: read, update, delete
# user authority: three digits, 111--can read , update, delete, 100--can read, cannot update, cannot delete
# user request operation: 100-read, 010--update, 001, delete
# suppose each client  only can request once each time
# database user account database and file database 


from urlparse import urlparse, parse_qsl
from pymongo import MongoClient
import Queue


urlString_1='http://localhost:8000/?userID=54635&operator=read&resourceID=1002,1004,1007&key=name,author&value=Truth,Chris'
urlString_4='http://localhost:8000/?userID=54635&operator=read&resourceID=0&key=name,author&value=Truth,Chris'

urlString_2='http://localhost:8000/?userID=54635&operator=update&resourceID=1004,1007&key=name,price&value=Truth,35'
urlString_3='http://localhost:8000/?userID=54635&operator=delete&resourceID=1007&key=name,price&value=Truth,33'
urlString_5='http://localhost:8000/?userID=54635&operator=delete&resourceID=0&key=name,price&value=Truth,35'




class Job:	
	def __init__(self, id, operator, keys, values, user):
		self.id = id 
		self.tag = False # the file is currently not in any use
		self.operator = operator
		self.keysArray = keys
		self.valuesArray = values
		self.userId = user



class Server():
	dataBase=[]
	cachedFiles=[]
	fileQueue=Queue.Queue()
	
	def startOperation(self, resArray, keysArray, valuesArray):
	#start processing jobs on the fileQueue
		while not self.fileQueue.empty():
			job = self.fileQueue.get() #queue operation, pop and get 
			if job.operator=="read":
				self.read(job, keysArray , valuesArray)
			if job.operator=="update":
				self.update(job, keysArray, valuesArray)
 			if job.operator=="delete":
 				self.delete(job, keysArray, valuesArray)
			
			
				

	def checkPriority(self,userId, operation, resources, obj):
	#check request from client has the authority to the operation or not, if yes, create a new job on the fileQueue
		
		userInfo =list(self.dataBase.Usertable.find({"userID":int(userId, 0)}))
		permission =  userInfo[0][u'permission']
		resArray = resources.split(",")
		keysArray = obj[3][1].split(",") # in the form of "name:"
		valuesArray = obj[4][1].split(",")
		
		for item in range(0, len(resArray)):			
			if operation=="read" and permission/100==1:
				self.fileQueue.put(Job(resArray[item],"read",keysArray, valuesArray,userId))

			if operation=="update" and abs(permission-100)/10>=1:
				self.fileQueue.put(Job(resArray[item],"update",keysArray, valuesArray,userId))

				#self.update(resources,urlObj)
			
			if operation=="delete" and abs(permission-110)/1>=1:
				self.fileQueue.put(Job(resArray[item],"delete",keysArray, valuesArray,userId))
				#self.delete(resources,urlObj)
		if not self.fileQueue.empty():
			self.startOperation(resArray, keysArray, valuesArray)
		
		
		
		
	def getURL(self, urlString):
		check = True;
		urlObj =  parse_qsl(urlparse(urlString)[4])	
		self.checkPriority(urlObj[0][1],urlObj[1][1], urlObj[2][1],urlObj)
		
		
		
	def read(self,job, keysArray, valuesArray ):
		print "I'm reading " + job.id	
		query={}
		if len(keysArray)==0:
			print list(dataBase.Filetable.find())
		else:
			if job.id!='0':
				query["fileID"]=int(job.id,0)
			for item in range(0,len(keysArray)):
				query[keysArray[item]]=valuesArray[item]
			print "The query condition is ..."
			print query
			result = list(self.dataBase.Filetable.find(query))
			print result		
		return result
	
	
	
	
	def update(self,job, keysArray, valuesArray):
		print "I'm updating "+job.id
		print "Before update..."
		print list(self.dataBase.Filetable.find({"fileID":int(job.id,0)}))	
		for keys in range(0, len(keysArray)):		
			self.dataBase.Filetable.update({"fileID":int(job.id,0)},{'$set':{keysArray[keys]:valuesArray[keys]}})			
		print "After update..."			
		print list(self.dataBase.Filetable.find({"fileID":int(job.id,0)}))
			
		return self.dataBase
		
		
		
		
	def delete(self,job, keysArray, valuesArray):
	# return the deleted message,find cached files first, if not, fetch from dB
		print "I'm deleting"+job.id

		if len(keysArray)==0:      # no <key, value> specified
			print list(db.Filetable.find())
			self.dataBase.Filetable.remove({"fileID":int(job.id,0)},True)
					# no need to set tag back to False again, because the whole file is deleted
			print list(db.Filetable.find())
		else:
			query={}
			if job.id!='0':
				query["fileID"]=int(job.id,0)						
			for item in range(0,len(keysArray)):
				query[keysArray[item]]=valuesArray[item]
			print "The query condition is ..."					
			print query
			self.dataBase.Filetable.remove(query, False)
			print list(self.dataBase.Filetable.find(query))	
		return 
		
	

	def getDB(self):
		from pymongo import MongoClient
    	client = MongoClient('localhost:27017')
    	dataBase = client.DolbyDB
		
	
			
		
		

server = Server()
server.getDB()
print "********** get url_1 **********"
server.getURL(urlString_1)
print "********** get url_4 **********"
server.getURL(urlString_4)
print "********** get url_2 **********"

server.getURL(urlString_2)
print "********** get url_3 **********"
 
server.getURL(urlString_3)
print "********** get url_5 **********"
server.getURL(urlString_5)






