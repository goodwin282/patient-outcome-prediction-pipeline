#!/usr/bin/env python3
"""
Script to create AWS S3 buckets for a healthcare data lake with bronze, silver, and gold layers
For the Patient Outcome Prediction Pipeline project
"""

import boto3
import json
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set variables
PROJECT_NAME = "patient-outcome"
AWS_REGION = "us-west-2"  # Change this to your preferred region
ENVIRONMENT = "dev"       # Options: dev, staging, prod

# Create unique, compliant bucket names with proper naming convention
BRONZE_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-bronze-data"
SILVER_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-silver-data"
GOLD_BUCKET = f"{PROJECT_NAME}-{ENVIRONMENT}-gold-data"

def create_bucket(bucket_name, region, tier):
    """
    Create an S3 bucket with appropriate settings for healthcare data
    
    Parameters:
        bucket_name (str): Name of the bucket to create
        region (str): AWS region to create the bucket in
        tier (str): Data tier (bronze, silver, gold) to set appropriate policies
    
    Returns:
        bool: True if bucket was created or already exists, False on error
    """
    s3_client = boto3.client('s3', region_name=region)
    
    logger.info(f"Creating {tier} tier bucket: {bucket_name}")
    
    # Create the bucket with the appropriate region configuration
    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        logger.info(f"Bucket {bucket_name} created successfully")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            logger.info(f"Bucket {bucket_name} already exists and is owned by you")
        else:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False
    
    # Enable versioning for data protection
    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        logger.info(f"Enabled versioning on {bucket_name}")
    except ClientError as e:
        logger.error(f"Error enabling versioning on {bucket_name}: {e}")
        return False
    
    # Enable default encryption for HIPAA compliance
    try:
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        },
                        'BucketKeyEnabled': True
                    }
                ]
            }
        )
        logger.info(f"Enabled encryption on {bucket_name}")
    except ClientError as e:
        logger.error(f"Error enabling encryption on {bucket_name}: {e}")
        return False
    
    # Block public access to all buckets (healthcare data security)
    try:
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            }
        )
        logger.info(f"Blocked public access on {bucket_name}")
    except ClientError as e:
        logger.error(f"Error blocking public access on {bucket_name}: {e}")
        return False
    
    # Set lifecycle policies based on tier
    lifecycle_config = get_lifecycle_config(tier)
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        logger.info(f"Set lifecycle policy for {tier} tier on {bucket_name}")
    except ClientError as e:
        logger.error(f"Error setting lifecycle policy on {bucket_name}: {e}")
        return False
    
    # Add HIPAA compliance tags
    try:
        s3_client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'Project',
                        'Value': 'PatientOutcomePrediction'
                    },
                    {
                        'Key': 'Environment',
                        'Value': ENVIRONMENT
                    },
                    {
                        'Key': 'DataTier',
                        'Value': tier
                    },
                    {
                        'Key': 'Contains-PHI',
                        'Value': 'True'
                    },
                    {
                        'Key': 'Compliance',
                        'Value': 'HIPAA'
                    }
                ]
            }
        )
        logger.info(f"Added compliance tags to {bucket_name}")
    except ClientError as e:
        logger.error(f"Error adding tags to {bucket_name}: {e}")
        return False
    
    return True

def get_lifecycle_config(tier):
    """
    Returns the appropriate lifecycle configuration based on the data tier
    
    Parameters:
        tier (str): Data tier (bronze, silver, gold)
    
    Returns:
        dict: Lifecycle configuration
    """
    if tier == "bronze":
        # For raw data - move to infrequent access after 60 days, glacier after 180 days
        return {
            'Rules': [
                {
                    'ID': 'Move to IA and Glacier',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},
                    'Transitions': [
                        {
                            'Days': 60,
                            'StorageClass': 'STANDARD_IA'
                        },
                        {
                            'Days': 180,
                            'StorageClass': 'GLACIER'
                        }
                    ]
                }
            ]
        }
    elif tier == "silver":
        # For processed data - move to infrequent access after 30 days, glacier after 90 days
        return {
            'Rules': [
                {
                    'ID': 'Move to IA and Glacier',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},
                    'Transitions': [
                        {
                            'Days': 30,
                            'StorageClass': 'STANDARD_IA'
                        },
                        {
                            'Days': 90,
                            'StorageClass': 'GLACIER'
                        }
                    ]
                }
            ]
        }
    else:  # gold
        # For analytics-ready data - move to infrequent access after 30 days
        return {
            'Rules': [
                {
                    'ID': 'Move to IA',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},
                    'Transitions': [
                        {
                            'Days': 30,
                            'StorageClass': 'STANDARD_IA'
                        }
                    ]
                }
            ]
        }

def create_data_lake():
    """
    Create all three tiers of the data lake
    """
    buckets = {
        "bronze": BRONZE_BUCKET,
        "silver": SILVER_BUCKET,
        "gold": GOLD_BUCKET
    }
    
    success = True
    for tier, bucket_name in buckets.items():
        if not create_bucket(bucket_name, AWS_REGION, tier):
            success = False
    
    if success:
        logger.info("✅ Data lake S3 buckets created successfully!")
        logger.info(f"Bronze tier: {BRONZE_BUCKET} (raw data storage)")
        logger.info(f"Silver tier: {SILVER_BUCKET} (processed/transformed data)")
        logger.info(f"Gold tier: {GOLD_BUCKET} (analytics-ready data)")
        
        # Output bucket information to a configuration file for reference
        config = {
            "project": PROJECT_NAME,
            "environment": ENVIRONMENT,
            "region": AWS_REGION,
            "buckets": {
                "bronze": BRONZE_BUCKET,
                "silver": SILVER_BUCKET,
                "gold": GOLD_BUCKET
            }
        }
        
        with open('data_lake_config.json', 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info("Configuration saved to data_lake_config.json")
        return True
    else:
        logger.error("❌ Failed to create some data lake buckets")
        return False

if __name__ == "__main__":
    create_data_lake()