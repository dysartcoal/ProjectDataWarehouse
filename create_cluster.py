#!/usr/bin/env python3

import pandas as pd
import boto3
import json
import configparser
import os
import datetime
import psycopg2

from botocore.exceptions import ClientError

outfilename = "create_cluster.out"
outfile = open("./" + outfilename, "w+")
delconfig_filename = "redshift_cluster_dwh_delete.cfg"
createconfig_filename = "redshift_cluster_dwh_create.cfg"

tag_reasonudacity = {'Key':'reason', 'Value':'udacity'}


config = configparser.ConfigParser()
config.read_file(open(createconfig_filename))


#  Config values
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_REGION     = config.get("DWH","DWH_CLUSTER_REGION")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)


# AWS Clients/Resources
ec2 = boto3.resource('ec2',
                  region_name=DWH_CLUSTER_REGION,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

s3 = boto3.resource('s3',
                  region_name=DWH_CLUSTER_REGION,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

iam = boto3.client('iam',
                  region_name=DWH_CLUSTER_REGION,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

redshift = boto3.client('redshift',
                  region_name=DWH_CLUSTER_REGION,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

# Create the IAM role
try:
    print("Creating role\n")
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description='Allows Redshift clusters to call AWS services on your behalf.',
        AssumeRolePolicyDocument=json.dumps(
            {"Statement": [{"Action": "sts:AssumeRole",
                           "Effect": "Allow",
                           "Principal": {"Service": "redshift.amazonaws.com"}
                           }],
            "Version": "2012-10-17"})
    )
except Exception as e:
    print(e)

# Attach Policy
try:
    print('Attaching Policy\n')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                     PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                     )["ResponseMetadata"]["HTTPStatusCode"]
except Exception as e:
    print(e)
    
# Get and print the IAM role ARN
print('Get the IAM role ARN\n')
roleArn =  iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
outfile.write(f"{datetime.datetime.now()}: roleArn: {roleArn}")


print("Create the cluster...")
# Create cluster
try:
    response = redshift.create_cluster(        
        # hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # identifiers & credentials
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        # role (to allow s3 access)
        IamRoles=[roleArn],
        Tags=[tag_reasonudacity]
    )
except Exception as e:
    print(e)

# Wait until the cluster is available
redshift_available_waiter = redshift.get_waiter("cluster_available")
try:
    redshift_available_waiter.wait(
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        WaiterConfig={
            "Delay": 60,
            "MaxAttempts": 30
        }
    )
except botocore.exceptions.waiterError as e:
    if "Max attempts exceeded" in e.message:
        print("Cluster did not become available in 30 waits of 1 minute.")
    else:
        print(e)
    sys.exit()
    # Break from script if cluster not created
        
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

endpoint = myClusterProps['Endpoint']['Address']
DWH_ENDPOINT = endpoint
roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']  # Should be the same as via iam.get_role
outfile.write(f"{datetime.datetime.now()}: (from cluster) roleArn: {roleArn}\n")
outfile.write(f"{datetime.datetime.now()}: endpoint: {endpoint}\n")

# THE FOLLOWING PUBLICLY OPEN SYSTEM CONFIGURATION IS NOT RECOMMENDED FOR FUTURE - ONLY USED FOR TRAINING
# Configure tcp access to cluster
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    # Ingress available to everyone!
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,  
        CidrIp='0.0.0.0/0', 
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
    
# Check can connect to cluster
try:
    conn=psycopg2.connect(dbname=DWH_DB, host=DWH_ENDPOINT, port=DWH_PORT, user=DWH_DB_USER, password=DWH_DB_PASSWORD)
    cursor=conn.cursor()
    cursor.execute("""select tablename from pg_table_def""")
    rows = cursor.fetchall()
    print("Successfully fetched the following row: \n")
    print(rows[0])
    conn.close()
except Exception as e:
    print(e)
    
# Create config file for deletion of resources
with open("./" + delconfig_filename, "w+") as config_file:
    config_file.write("[AWS]\n")
    config_file.write("KEY="+KEY+"\n")
    config_file.write("SECRET="+SECRET+"\n")
    config_file.write("\n[DWH]\n")
    config_file.write("DWH_CLUSTER_IDENTIFIER="+DWH_CLUSTER_IDENTIFIER+"\n")
    config_file.write("DWH_CLUSTER_REGION="+DWH_CLUSTER_REGION+"\n")
    config_file.write("DWH_IAM_ROLE_NAME="+DWH_IAM_ROLE_NAME+"\n")


outfile.close()