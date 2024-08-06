import redis

# Initialize Redis client
redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

# add data to Redis
redis_client.set("key", "value")

# get data from Redis
data = redis_client.get("key")
print(data)
