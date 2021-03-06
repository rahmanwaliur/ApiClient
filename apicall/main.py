__author__ = 'rahman'

import thread                                                               #import thread module for threading
from src.onavinterface import OnavInterface                                 #import OnavInterface class
from src.workflowengine import WorkFlowEngine                               #import WorkFlowEngine class
from src.hquerygeneration import HQueryGeneration

#main method
def main():

    interfaceObj = OnavInterface()                                             #object creation for OnavInterface class
    engineObj = WorkFlowEngine()                                               #object creation for WorkFlowEngine class
    hivequeryObj = HQueryGeneration()                                          #object creation for HQueryGeneration class

    onavresponse = interfaceObj.getResponse()                                  #store Onav joblist in onavresponse
    querydata = engineObj.parseResponse(onavresponse)                          #parse onavresponse and store querydata in raw format
    #query = engine.generateQuery(querydata)
    query = hivequeryObj.generateQuery(querydata)                              #generate hive query based on querydata
    flag = 1                                                                #this flag is used to control the thrift connection


    if isinstance(query,list):                                              #check if there are multiple queries or pending jobs received from onav
        length = len(query)                                                 #determine number of pending job requests
        for x in range(0,length):                                           #for each job do the following
            if length == x+1:                                               #change flag value if this is the last job
                flag = 0
            thread.start_new_thread(engineObj.processQuery,(query[x],flag))    #submit job as a thread through engine.processQuery
            engineresponse = engineObj.retrieveJobID(x)                        #result(e.g. jobID) after job submission
            interfaceObj.sendResult(engineresponse)                            #sending result back to ONav
    else:
        length = 0                                                             #execute if there is only one job
        thread.start_new_thread(engineObj.processQuery,(query,flag))           #submit job as a thread through engine.processQuery
        engineresponse = engineObj.retrieveJobID(length)                       #result(e.g. jobID) after job submission
        interfaceObj.sendResult(engineresponse)                                #sending result back to ONav


if __name__== "__main__":
    main()

