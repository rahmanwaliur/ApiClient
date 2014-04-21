__author__ = 'rahman'
import logging
import requests
from requests import exceptions
import json

class OnavInterface:
    'this class requests for joblist from ONav API and communicate results after job submission'

    #constructor
    def __init__(self):
        self.response = ''

    #api call to ONav to pull list of pending jobs
    def getResponse(self):

        url = 'https://cru.ucalgary.ca/getpendinglist'
        try:
            self.response = requests.get(url)

            if self.response.get('errors'):
                logging.warn("API error response")
                return {'request_error': 'api_error_response'}
        except requests.exceptions.ConnectionError:
            logging.warn('ConnectionError')
            return {'request_error': 'ConnectionTimeout'}
        except requests.exceptions.Timeout:
            logging.warn('API request timed out')
            return {'request_error': 'Timeout'}
        except Exception, ex:
            logging.warn("API request exception: %s", ex)
            return {'request_error':ex}
        else:
            return self.response

       #with open("src/data.json") as filename:
       #    parsed_data = json.loads(filename.read())  # all parsed data requests in json format
       #    self.response = parsed_data
       #    return self.response



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



