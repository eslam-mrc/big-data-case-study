import requests
import json
import sys

AMBARI_DOMAIN='127.0.0.1'
AMBARI_PORT='8080'
AMBARI_USER_ID='raj_ops'
AMBARI_USER_PW='raj_ops'
CLUSTER_NAME = 'Sandbox'
RM_DOMAIN='127.0.0.1'
RM_PORT='8088'

restAPI='/api/v1/clusters'

#This function establishes connection with the Ambari API
def ambariREST( restAPI ) :
    url = "http://"+AMBARI_DOMAIN+":"+AMBARI_PORT+restAPI
    r= requests.get(url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    return(json.loads(r.text));


#This function return some info about a single service
def getService( SERVICE) :
    restAPI = "/api/v1/clusters/"+CLUSTER_NAME+"/services/"+SERVICE
    json_data =  ambariREST(restAPI)
    serviceName = json_data["ServiceInfo"]["service_name"]
    serviceState = json_data["ServiceInfo"]["state"]
    alertsSummary = json_data["alerts_summary"]["WARNING"]
    row = {"service_name":serviceName, "state":serviceState, "alerts_summary":alertsSummary}
    return(row); 


#This function returns all services and their status on the cluster
def getServices() :
    restAPI = "/api/v1/clusters/"+CLUSTER_NAME+"/services"
    json_data =  ambariREST(restAPI)
    serviceList = list()
    returnList = list()
    for item in json_data["items"]:
        service = item["ServiceInfo"]["service_name"]
        serviceList.append(service)
    for service in serviceList:
        returnList.append(getService(service))
    for row in returnList:
        print(row)
 
 #This function returns cluster name, stack name and version
def getStackVersions() :
    restAPI = "/api/v1/clusters/"+CLUSTER_NAME+"/stack_versions/"
    json_data =  ambariREST(restAPI)
    row = {"cluster_name":json_data["items"][0]["ClusterStackVersions"]["cluster_name"]\
    , "stack":json_data["items"][0]["ClusterStackVersions"]["stack"]\
    , "version":json_data["items"][0]["ClusterStackVersions"]["version"] }
    print(row)
    
ans=True
while ans:
    print ("""
    1.Check All Services
    2.Check a Service
    3.Cluster Name & Stack Version
    4.Exit/Quit
    """)
    ans=input("What would you like to do? ") 
    if ans=="1":
        getServices()    
      #print("\n Student Added") 
    elif ans=="2":
        serviceName = input("Enter Service Name: ")
        if serviceName !="":
            try:
                print(getService(serviceName))
            except:
                print("You have either misspelled the service or it doesn't exit, please try again")
                pass
                #sys.exit()
      #print("\n Student Deleted") 
    elif ans=="3":
        getStackVersions()
      #print("\n Student Record Found") 
    elif ans=="4":
        print("\n Have a good day. Goodbye!")
        sys.exit()     
    elif ans !="":
        print("\n Not Valid Choice Try again") 
   

	
	
	
	
