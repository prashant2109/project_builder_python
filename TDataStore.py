import os
import sys
import redis

class TDataStore:
    def __init__(self, ip='localhost:6379', pwd='', db_id=0):
        try:
            self.address = ip
            splitted = ip.split(':')
            self.ipAdd = splitted[0]
            self.ipPort  = int(splitted[1])
            self.db = db_id
        except:
             print >> sys.stderr , "TDataStore Error in IP:PORT syntax"
        try:
            if not pwd:
                print 'while getting conn', self.ipAdd,self.ipPort,self.db
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, db=self.db)
            else:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, password=pwd)
        except:
            print >> sys.stderr , "TDataStore Error connecting to ", self.ipAdd+':'+self.ipPort
        return

    #Returns a dictionary containing information about the Redis server. The  'section' option can be used to select a specific section
    def info(self, section=None):
        if section is None:
            return self.redisConn.execute_command('INFO')
        else:
            return self.redisConn.execute_command('INFO', section) 
            
    #Ping the Redis server
    def ping(self):
        try:
            return self.redisConn.execute_command('PING')
        except:
            print >> sys.stderr , "TDataStore Error while pinging ", self.ipAdd+':'+self.ipPort
            return

    #Tell the Redis server to save its data to disk, blocking until the save is complete
    def save(self):
        try:
            return self.redisConn.execute_command('SAVE')
        except:
            print >> sys.stderr , "TDataStore Error while saving to disk!"
            return

    #Shutdown the server
    def serverShutdown(self):
        try:
            self.redisConn.execute_command('SHUTDOWN')
        except redis.ConnectionError:
            return
        raise RedisError("TDataStore Error! SHUTDOWN seems to have failed!")

    #Returns the server time as a 2-item tuple of ints: (seconds since epoch, microseconds into this second).
    def getTime(self):
        try:
            return self.redisConn.execute_command('TIME')
        except:
            print >> sys.stderr , "TDataStore Error while executing Redis TIME command!"
            return

    #Delete one or more keys specified by 'names'
    def deleteRecord(self, *names):
        try:
            return self.redisConn.execute_command('DEL', *names)
        except:
            print >> sys.stderr , "TDataStore Error while deleting record!"
            return

    #Delete keys which matches the pattern
    def deleteMatchingKeys_original(self, pattern):
        try:
            return self.redisConn.execute_command('DEL', *(self.redisConn.execute_command('KEYS', pattern)))
        except(redis.exceptions.ResponseError):
            print >> sys.stderr , "TDataStore Error! No Keys match the given pattern"
        return

    def deleteMatchingKeys(self, pattern):
        try:
            cursor = 0
            while True:
                res_tup = self.redisConn.execute_command('SCAN', cursor, 'MATCH', pattern)
                cursor = res_tup[0]
                if res_tup[1]:
                    self.redisConn.execute_command('DEL', *res_tup[1])
                if cursor == 0:
                    break;
        except(redis.exceptions.ResponseError):
            print >> sys.stderr , "TDataStore Error! Error while deleting keys with given pattern"
        return

    #Return the value at key 'name', or None if the key doesn't exist
    def getRecord(self, name):
        print 'while getting records', self.ipAdd,self.ipPort,self.db
        return self.redisConn.execute_command('GET', name)
        try:
            return self.redisConn.execute_command('GET', name)
        except:
            print >> sys.stderr , "TDataStore Error! No Key matches the given pattern"       
    def isKey(self,name):
        return self.redisConn.execute_command('EXISTS', name)
    

    
    #Returns a list of keys matching 'pattern'
    def getKeys(self, pattern='*'):
       try:
            return self.redisConn.execute_command('KEYS', pattern)
       except :
            print >> sys.stderr , "TDataStore Error while getting KEYS!"
       return
    
    #Rename key 'src' to 'dst'
    def renameKey(self, src, dst):
        try:
            return self.redisConn.execute_command('RENAME', src, dst)
        except:
            print >> sys.stderr , "TDataStore Error while renaming key!"
        return
    
    #Set the value at key 'name' to 'value' if key already exists else creates a new Record
    def createRecord(self, name, value):
        record = [name, value]
        try:
            return self.redisConn.execute_command('SET', *record)
        except Exception as e:
            print >> sys.stderr , "TDataStore Error while creating record!"

        return

    #Returns the type of key 'name'
    def keyType(self, name):
        try:
            return self.redisConn.execute_command('TYPE', name)
        except:
            print >> sys.stderr , "TDataStore Error while executing KEY TYPE!"
    
    def startLocalRedisServer(self):
        try:
            os.system('systemctl start redis')
        except:
            print >> sys.stderr , "TDataStore Error while starting local Redis Server!"
        return
    
    #Connect to Server; address  in the form 'xxx.xxx.xxx.xxx:port'
    def getConnection(self, address, pwd='', db_id=0):
        try:
            splitted = address.split(':')
            self.ipAdd = splitted[0]
            self.ipPort  = int(splitted[1])
            self.db = db_id
        except:
             print >> sys.stderr , "TDataStore Error in IP:PORT syntax!"
             return
        try:
            if not pwd:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, db=self.db)
            else:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, password=pwd)
        except:
            print >> sys.stderr , "TDataStore GetConnection Error while connecting to ", self.ipAdd+':'+self.ipPort, " with DB ID: ", self.db
        return
    
    #Returns information of the connected clients
    def getClientList(self):
        return self.redisConn.execute_command("CLIENT LIST")
    
    #Switch DB by id
    def selectDB(self, db_id=0, pwd=''):
        self.db = db_id
        try:
            if not pwd:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, db=self.db)
            else:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, password=pwd)
        except:
            print >> sys.stderr , "TDataStore DBID Error while connecting to ", self.ipAdd+':'+self.ipPort, " with DB ID: ", self.db
        return

    #Switch DB by port
    def selectPort(self, port, pwd=''):
        self.ipPort = port
        try:
            if not pwd:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, db=self.db)
            else:
                self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, password=pwd)
        except:
            print >> sys.stderr , "TDataStore PortError while connecting to ", self.ipAdd+':'+self.ipPort, " with DB ID: ", self.db
        return
    
    #Deletes current DB
    def deleteDB(self):
        try:
            return self.redisConn.execute_command('FLUSHALL')
        except:
            print >> sys.stderr , "TDataStore Error while deleting DB ", self.ipAdd+':'+self.ipPort, " with DB ID: ", self.db
    
    #Sets password for the DB
    def setPassword(self, password, hashed=False):
        if hashed:
            password = self.hashData(password)        
        try:
            return self.redisConn.execute_command('CONFIG SET', 'requirepass', password)
        except:
            print >> sys.stderr , "TDataStore Error while setting password in DB " , self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
        return
#########################SET##########################

    def sAdd(self, name, *values):
        try:
            return self.redisConn.execute_command('SADD', name, *values)
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while sadd in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
    def sMembers(self, name):
        try:
            return self.redisConn.execute_command('SMEMBERS', name)
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while sMembers in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
    
#########################LIST#########################
    def lPush(self, listname, values):
        try:            
            self.redisConn.execute_command('LPUSH', listname, *values)
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while lpush in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
    
    def rPush(self, listname, values):
        try:
            self.redisConn.execute_command('LPUSH', listname, *values)
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while rpush in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
    
    def lPop(self, listname):
        try:
            res = self.redisConn.execute_command('LPOP', listname)
            return res
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while lpop in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
    
    
    def rPop(self, listname):
        try:
            res = self.redisConn.execute_command("RPOP", listname)
            return res
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while rpop in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e

    def blPop(self, keys, timeout=0):
        try:
            if timeout is None:
                timeout = 0
            keys = list_or_args(keys, None)
            keys.append(timeout)
            return self.redisConn.execute_command('BLPOP', *keys)
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while blpop in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
        return
    
    def brPop(self, keys, timeout=0):
        try:
            if timeout is None:
                timeout = 0
            keys = list_or_args(keys, None)
            keys.append(timeout)
            return self.redisConn.execute_command("BRPOP", *keys)
        except Exception as e:
            print >> sys.stderr, "TDataStore Error while brpop in DB ", self.ipAdd, ':', self.ipPort ," with DB ID: " , self.db
            print e
        return        






#############test each func###################
#redisObj = TDataStore('127.0.0.1:6380')

#redisObj.lpush("test", "suvarna")
#redisObj.sAdd("set_test", "suvarna", "khanal")





#data1 = redisObj.encryptData('')
#print data1
#real = redisObj.decryptData(data1)
#print real
#print redisObj.encrypt('suvarnakhanal')
#print redisObj.setPassword('ranjeet')
#redisObj.getKeys()
#print redisObj.startLocalRedisServer
#print redisObj.keyType('test')
#print redisObj.renameKey('dav', 'vad')
#redisObj.getKeys()
#print redisObj.deleteRecord('test')
#print redisObj.getTime()
#print redisObj.serverShutdown()
#print redisObj.startLocalRedisServer
#print redisObj.ping()
#print redisObj.info()
#print redisObj.save()
#print redisObj.deleteMatchingKeys('*a*')
#redisObj.getConnection('192.168.4.8080')
#print redisObj.getClientList()
#print redisObj.selectDB(1)
#print redisObj.createRecord('db1', 36)
#redisObj.getKeys()
#redisObj.selectPort(631)
#print redisObj.createRecord('newport', 1)
#redisObj.getKeys()
#redisObj.deleteDB()
##############################################









