__author__ = 'rahman'
import os
import unittest
from apicall.src.onavinterface import OnavInterface
from mock import patch, Mock, MagicMock
import requests
import json

from mocker import Mocker, MockerTestCase


def mock_APIresponse_for_OnavInterface_getResponse():

    with open("/home/rahman/PycharmProjects/ApiClient/apicall/src/data.json") as filename:
        response = json.loads(filename.read())
    return response


class TestOnavInterface(unittest.TestCase):

    def setUp(self):
        pass

    def test_response_in_GetResponse(self):

        expected = {u'cohortname': u'test1cohort', u'medcodes': [u'c68710004', u'c68710002', u'c68710001', u'c68710006', u'c68710005', u'c68710007'], u'userid': u'waliur', u'jobid': 10}
        requests.get = MagicMock(side_effect=mock_APIresponse_for_OnavInterface_getResponse())
        responseObj = OnavInterface()
        response = responseObj.getResponse()
        #expected = json.dumps(expected)
        self.assertEqual(response, expected)

    def test_getResponse(self):
        pass
        #self.fail()


    def test_sendResult(self):
        pass
        #self.fail()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()