__author__ = 'rahman'


from unittest import TestCase

from apicall.src.workflowengine import WorkFlowEngine



class TestWorkFlowEngine(TestCase):

    def test_parseResponse(self):
        response = [{u'cohortname': u'test1cohort', u'medcodes': [u'c68710004', u'c68710002', u'c68710001', u'c68710006', u'c68710005', u'c68710007'], u'userid': u'waliur', u'jobid': 10},{u'cohortname': u'test2cohort', u'medcodes': [u'c68710004', u'c68710002', u'c68710001', u'c68710006', u'c68710005', u'c68710007'], u'userid': u'jordan', u'jobid': 11}]
        expectedresponse = {'cohort':[u'test1cohort', u'test2cohort'],'codes':[[u'c68710004', u'c68710002', u'c68710001', u'c68710006', u'c68710005', u'c68710007'], [u'c68710004', u'c68710002', u'c68710001', u'c68710006', u'c68710005', u'c68710007']] ,'user':[u'waliur', u'jordan'], }
        responseObj = WorkFlowEngine()
        receivedresponse = responseObj.parseResponse(response)
        self.assertEqual(receivedresponse,expectedresponse)

    def test_generateQuery(self):
        self.fail()

    def test_processQuery(self):
        self.fail()

    def test_retrieveJobID(self):
        self.fail()