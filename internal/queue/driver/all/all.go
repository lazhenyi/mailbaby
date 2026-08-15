package all

import (
	_ "mailbaby/internal/queue/driver/kafka"
	_ "mailbaby/internal/queue/driver/nats"
	_ "mailbaby/internal/queue/driver/pulsar"
	_ "mailbaby/internal/queue/driver/rabbitmq"
	_ "mailbaby/internal/queue/driver/redis"
	_ "mailbaby/internal/queue/driver/rocketmq"
	_ "mailbaby/internal/queue/driver/sqs"
)
