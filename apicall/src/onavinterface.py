__author__ = 'rahman'

import requests
import json

class OnavInterface:
    'this class requests for joblist from ONav API and communicate results after job submission'

    #constructor
    def __init__(self):
        self.response = ''

    #api call to ONav to pull list of pending jobs
    def getResponse(self):
       #self.response = requests.get('https://github.com/timeline.json')
       with open("src/data.json") as filename:
           parsed_data = json.loads(filename.read())  # all parsed data requests in json format
           self.response = parsed_data
           return self.response



    #api call to ONav to send hadoop jobid for corresponding ONav jobid
    def sendResult(self, allids):
         #responsestring = {"id": parsed_job["jobid"], "jobId": jobID}
         response = {"id": allids["onavjobid"], "jobID":allids["hadoopjobid"]}
         print response
         #url = 'http://localhost:8000/job/create/?'
         # ##for key, value in responsestring.iteritems():
         # ##    url += '%s=%s&' % (key, value)
         # ##url = url[:-1]
         # #self.re
         # ##requests.get(url)



