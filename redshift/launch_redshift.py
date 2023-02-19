import time
import pandas as pd
import boto3
import json
import configparser

# Read Config File
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
REGION                 = config.get('AWS','REGION')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

# Create Clients
ec2 = boto3.client('ec2', region_name=REGION, aws_access_key_id = KEY, aws_secret_access_key = SECRET)

s3 = boto3.client('s3', region_name=REGION, aws_access_key_id = KEY, aws_secret_access_key = SECRET)

iam = boto3.client('iam', region_name=REGION, aws_access_key_id = KEY, aws_secret_access_key = SECRET)

redshift = boto3.client('redshift', region_name=REGION, aws_access_key_id = KEY, aws_secret_access_key = SECRET)

# Create IAM Role
# TODO: Create the IAM role
try:
    print('Creating a new IAM Role')
    response = iam.create_role(
    RoleName=DWH_IAM_ROLE_NAME,
    AssumeRolePolicyDocument=json.dumps(
        { 'Statement': [
            {
            'Effect': 'Allow',
            'Principal': {'Service':'s3.amazonaws.com'},
            'Action': 'sts:AssumeRole'
            
          }]
      }))
    print("IAM role created")
except Exception as e:
    print(str(e))

try:
    print('Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    print("IAM policy attached.")
except Exception as e:
    print(str(e))

print('Get the IAM role ARN')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print("Role ARN -> ", roleArn)

# Create Redshift cluster
try:
    response = redshift.create_cluster(        
        # HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        
        # Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles
        IamRoles = [roleArn]
         
    )
except Exception as e:
    print(e)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

while True:
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0] 
    if myClusterProps["ClusterStatus"].lower()=="available":
        break
    print("Waiting for cluster to come up...")
    time.sleep(10)

print(prettyRedshiftProps(myClusterProps))

# Open TCP Port to access dwh
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

try:
    ec2 = boto3.resource('ec2', region_name=REGION, aws_access_key_id = KEY, aws_secret_access_key = SECRET)
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)