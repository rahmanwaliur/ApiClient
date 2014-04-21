__author__ = 'rahman'
import os
import unittest
from apicall.src.onavinterface import OnavInterface
from mock import patch, Mock, MagicMock
import requests
from requests import exceptions
import json

from mocker import Mocker, MockerTestCase


def mock_APIresponse_for_OnavInterface_getResponse():

    with open("/home/rahman/PycharmProjects/ApiClient/apicall/src/data.json") as filename:
        response = json.loads(filename.read())
    return response

def mock_ConnectionError_for_OnavInterface_getResponse():
    return requests.exceptions.ConnectionError

class TestOnavInterface(unittest.TestCase):

    def setUp(self):
        pass

    def test_requests_dot_get_in_GetResponse(self):

        responseObj = OnavInterface()
        with patch('requests.get') as patched_get:
            requests.get = MagicMock(side_effect=mock_APIresponse_for_OnavInterface_getResponse())
            responseObj.getResponse()
            # Ensure patched get was called and called only once
            patched_get.assert_called_with_once("https://cru.ucalgary.ca/getpendinglist")


    def test_APIresponse_in_GetResponse(self):

        expected = {u'cohortname': u'test1cohort', u'medcodes': [u'c68710004', u'c68710002', u'c68710001', u'c68710006', u'c68710005', u'c68710007'], u'userid': u'waliur', u'jobid': 10}
        requests.get = MagicMock(side_effect=mock_APIresponse_for_OnavInterface_getResponse())
        responseObj = OnavInterface()
        response = responseObj.getResponse()
        self.assertEqual(response, expected)

    def test_ConnectionError_in_GetResponse(self):
        responseObj = OnavInterface()
        requests.get = MagicMock(side_effect=mock_ConnectionError_for_OnavInterface_getResponse())
        response = responseObj.getResponse()
        self.assertEqual(response, {'request_error': 'ConnectionTimeout'})


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