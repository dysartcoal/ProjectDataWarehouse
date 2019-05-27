#!/usr/bin/env python3

import pandas as pd
import boto3
import json
import configparser
import psycopg2

from botocore.exceptions import ClientError

deleterole = False;
outfilename = "delete_cluster.out"
outfile = open("./" + outfilename, "w+")
delconfig_filename = "redshift_cluster_dwh_delete.cfg"

tag_reasonudacity = {'Key':'reason', 'Value':'udacity'}

config = configparser.ConfigParser()
config.read_file(open(delconfig_filename))

#  Config values
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_REGION     = config.get("DWH","DWH_CLUSTER_REGION")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

print("Create iam and redshift clients")
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

print("Deleting cluster...")
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
# THE FOLLOWING PUBLICLY OPEN SYSTEM CONFIGURATION IS NOT RECOMMENDED FOR FUTURE - ONLY USED FOR TRAINING
# Configure tcp access to cluster

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

redshift_deleted_waiter = redshift.get_waiter("cluster_deleted")
try:
    redshift_deleted_waiter.wait(
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        WaiterConfig={
            "Delay": 60,
            "MaxAttempts": 30
        }
    )
except botocore.exceptions.waiterError as e:
    if "Max attempts exceeded" in e.message:
        print("Cluster did not reach state deleted in 30 checks at 1 minute intervals.")
    else:
        print(e)

print("The wait for deletion is over")

if deleterole:
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print(f"Role {DWH_IAM_ROLE_NAME} deleted")
else:
    print(f"Role {DWH_IAM_ROLE_NAME} was not deleted")

outfile.close()