__author__ = 'rahman'

import pyhs2                #pyhs2 provides channel for connecting to hive using thrift server
import subprocess           #used for checking running jobs from terminal
from properties import ConfigProperties

class WorkFlowEngine:
    'process a joblist, submits to hive, retrives running jobs and find jobid from running jobs'

    #constructor
    def __init__(self):
        self.jobparams = []
        self.cohortnames = []
        self.usernames = []
        #self.query = []
        self.onavjobid = []
        self.hadoopjobid = []

    #parse ONav response to get user, cohort and medcodes for each request
    def parseResponse(self, response):
        for t in response:
            self.jobparams.append(t["medcodes"])        #list of combids
            self.cohortnames.append(t["cohortname"])    #list of cohort names provided by ONav users
            self.usernames.append(t["userid"])          #list of ONav user ids
            self.onavjobid.append(t["jobid"])           #list of Onav job ids to match with hadoop job ids

        return {'user':self.usernames, 'cohort':self.cohortnames, 'codes':self.jobparams}   #returns a dict of request elements after parsing


    def processQuery(self, querylist,flag):

        propertyObj = ConfigProperties()
        hostname = propertyObj.localhivehost()
        portnumber = propertyObj.localhiveport()
        authentication = propertyObj.localhiveauthentication()
        username = propertyObj.localhiveuser()
        userpassword = propertyObj.localuserpassword()
        databasename = propertyObj.localhivedatabase()


        conn = pyhs2.connect(host=hostname, port = portnumber, authMechanism = authentication, user=username, password=userpassword, database = databasename)
        cur = conn.cursor()

        cur.execute(querylist['createdb'])
        cur.execute(querylist['workdb'])
        cur.execute(querylist['droptable1'])
        cur.execute(querylist['createtable1'])
        cur.execute(querylist['testcode'])
        cur.execute(querylist['droptable2'])
        cur.execute(querylist['createtable2'])

        if flag == 0:
            cur.close()
            conn.close()


    def  retrieveJobID (self,iterationnumber):

        propertyObj = ConfigProperties()
        subcommand = propertyObj.localsubprocess()
        proc = subprocess.Popen(subcommand, stdout=subprocess.PIPE)        #checks for running jobs
        alljobs, err = proc.communicate()                               #captures running jobs in alljobs
        linecount = alljobs.count('\n')                                 #counts the number of lines in alljobs
        lines = alljobs.splitlines()                                    #list of lines for a running job
        jobline = lines[linecount-1]                                    #selects the line that has job id, usually the last line
        jobIDIndex = jobline.find("job_")                               #finds out the starting index of job id using pattern
        self.hadoopjobid.append(jobline[jobIDIndex:jobIDIndex+21])      #stores the job id using job id length

        return {'onavjobid':self.onavjobid[iterationnumber], 'hadoopjobid':self.hadoopjobid[iterationnumber]}   #retuns dict of job ids


