'''
In case we have dev and test/prod environments, 
conditional imports can be handled here pointing to the cloud services.
import os
if os.getenv("ENV") in ["test", "prod"]:
    ObjectStoreClient = services.S3ObjectClient
    (...)
'''

import services as services
ObjectStoreClient = services.MinIOObjectClient
OLTPDatabaseClient = services.PostgresDBClient
VectorDatabaseClient = services.QdrantDBClient
ModelsRegistryClient = services.MlflowClient
MessageBrokerClient = services.RedisClient
ComputeClient = services.PySparkClient
