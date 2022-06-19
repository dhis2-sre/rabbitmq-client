#!/bin/bash


docker exec -it rabbitmq-rabbitmq-1 \
rabbitmqctl set_policy --vhost / \
    Q_TTL_DLX \
    '^queue_client_example$' \
    '{"message-ttl":10000,"dead-letter-exchange":"","dead-letter-routing-key":"dlx_queue_client_example"}' \
    --apply-to queues

