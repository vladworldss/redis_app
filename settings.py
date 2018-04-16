

# Redis
REDIS_SERVER_CONF = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0
}

QUEUE_LOCK = "QUEUE:LOCK"
QUEUE_MSG = "QUEUE:MSG"
QUEUE_ERROR = "QUEUE:ERROR"
QUEUE_CID = "QUEUE:CID"

# Lock expire timeout
LOCK_TIMEOUT = 1

# Timeout between msg generating
MSG_GEN_TIMEOUT = 0.5
