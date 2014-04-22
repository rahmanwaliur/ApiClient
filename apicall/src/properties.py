__author__ = 'rahman'

class ConfigProperties:

    def __init__(self):
        self.url = ''
        self.connectionparams = []
        self.subprocess = ''


    def apicallurl(self):
        self.url = 'https://cru.ucalgary.ca/getpendinglist'
        return self.url

    def apisendurl(self):
        self.url='http://localhost:8000/job/create/?'
        return self.url

    def hivehost(self):
        host ='136.159.79.112'
        return host

    def hiveport(self):
        port = 10000
        return port

    def hiveauthentication(self):
        authMechanism = "PLAIN"
        return authMechanism

    def hiveuser(self):
        user = 'wrahman'
        return user

    def userpassword(self):
        password = 'suman@ASE13'
        return password

    def hivedatabase(self):
        database = 'default'
        return database

    def localhivehost(self):
        host ='localhost'
        return host

    def localhiveport(self):
        port = 10000
        return port

    def localhiveauthentication(self):
        authMechanism = "PLAIN"
        return authMechanism

    def localhiveuser(self):
        user = 'hduser'
        return user

    def localuserpassword(self):
        password = 'suman@ASE13'
        return password

    def localhivedatabase(self):

        database = 'default'
        return database

    def localhostconnection(self):

        host = 'localhost'
        port = 10000
        authMechanism = "PLAIN",
        user='hduser'
        password ='suman@ASE13'
        database = 'default'

        return {host,port,authMechanism,user,password,database}

    def localsubprocess(self):

        self.subprocess = "/usr/local/hadoop/bin/hadoop", "job", "-list"
        return self.subprocess

    def remotesubprocess(self):
        self.subprocess = "/usr/lib/hadoop/bin/hadoop", "job", "-list"
        return self.subprocess
