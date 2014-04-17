__author__ = 'rahman'

import pyhs2                #pyhs2 provides channel for connecting to hive using thrift server
import subprocess           #used for checking running jobs from terminal


class WorkFlowEngine:
    'process a joblist, submits to hive, retrives running jobs and find jobid from running jobs'

    #constructor
    def __init__(self):
        self.jobparams = []
        self.cohortnames = []
        self.usernames = []
        self.query = []
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

    # generate hive query for each request from ONav tool
    def generateQuery(self,qdata):
        self.query = ["select country, gni from hdi where gni<2000", "select country, gni from hdi where gni<2000"]
        """
        user = ''
        cohort = ''
        medcodes = []

        length = len(qdata)

        for x in range(0,length-1):
            user = qdata["user"][x]
            cohort = qdata["cohort"][x]
            medcodes = qdata["codes"][x]

            #table = "%s.combids" % (user)
            table = "%s_combids" % (user)

            code_list = {}

            if not medcodes:
                return False

            medcodes.sort()
            last_fc = medcodes[0][0]
            code_list[last_fc] = []

            for code in medcodes:
                if code[0] == last_fc:
                    code_list[last_fc].append("medcode LIKE '%s__'" % code)
            else:
                last_fc = code[0]
                code_list[last_fc] = ["medcode LIKE '%s__'" % code]

            master_list = ''
            for key,value in code_list.iteritems():
                code_list[key] = "(medcode1 = '%s' AND (%s))" % (key, " OR ".join(value))

            master_list = " OR ".join(code_list.values())

            hive_tables = ['demography_norm', 'ahd_denorm', 'medical_denorm', 'pvi_norm', 'therapy_norm', 'consult_denorm', 'patient_norm']

            thin_tables = ['demography', 'ahd', 'medical', 'pvi', 'therapy', 'consult', 'patient']

            create_db = "CREATE DATABASE IF NOT EXISTS %s" % user
            working_db = "USE default"
            drop_table1 = "DROP TABLE IF EXISTS %s" % table
            create_table1 = "CREATE TABLE IF NOT EXISTS %s (combid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" % table
            testing_codes = "INSERT OVERWRITE TABLE %s SELECT DISTINCT combid FROM medical_norm WHERE %s" % (table,master_list)
            drop_table2 = "DROP TABLE IF EXISTS %s_%s" % (user,hive_tables)
            create_table12 = "CRETAE TABLE %s_%s ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS SELECT * FROM %s LEFT SEMI JOIN %s ON (combids.combid = %s.combid)" % (user, hive_tables, thin_tables, table, thin_tables)

            self.query.append({'createdb':create_db, 'workdb':working_db, 'droptable1': drop_table1, 'createtable1': create_table1, 'testcode':testing_codes, 'droptable2':drop_table2, 'createtable2':create_table12})
        """
        return self.query

    def processQuery(self, processquery,flag):


        #conn = pyhs2.connect(host='136.159.79.112', port = 10000, authMechanism = "PLAIN", user='wrahman', password='suman@ASE13', database='default')

        conn = pyhs2.connect(host='localhost', port = 10000, authMechanism = "PLAIN", user='hduser', password='suman@ASE13', database='default')

        cur = conn.cursor()

        #cur.execute(processquery['createdb'])
        #cur.execute(processquery['workdb'])
        #cur.execute(processquery['droptable1'])
        #cur.execute(processquery['createtable1'])
        #cur.execute(processquery['testcode'])
        #cur.execute(processquery['droptable2'])
        #cur.execute(processquery['createtable2'])
        cur.execute(processquery)
        if flag == 0:
            cur.close()
            conn.close()


    def  retrieveJobID (self,iterationnumber):
        #proc = subprocess.Popen(["/usr/lib/hadoop/bin/hadoop", "job", "-list"], stdout=subprocess.PIPE)        #checks for running jobs
        proc = subprocess.Popen(["/usr/local/hadoop/bin/hadoop", "job", "-list"], stdout=subprocess.PIPE)
        alljobs, err = proc.communicate()                               #captures running jobs in alljobs
        linecount = alljobs.count('\n')                                 #counts the number of lines in alljobs
        lines = alljobs.splitlines()                                    #list of lines for a running job
        jobline = lines[linecount-1]                                    #selects the line that has job id, usually the last line
        jobIDIndex = jobline.find("job_")                               #finds out the starting index of job id using pattern
        self.hadoopjobid.append(jobline[jobIDIndex:jobIDIndex+21])      #stores the job id using job id length

        return {'onavjobid':self.onavjobid[iterationnumber], 'hadoopjobid':self.hadoopjobid[iterationnumber]}   #retuns dict of job ids


