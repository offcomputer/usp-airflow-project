import services as services
ObjectStoreClient = services.MinIOObjectClient
OLTPDatabaseClient = services.PostgresDBClient
VectorDatabaseClient = services.QdrantDBClient
ModelsRegistryClient = services.MlflowClient
MessageBrokerClient = services.RedisClient