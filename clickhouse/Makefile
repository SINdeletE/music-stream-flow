COMPOSE = docker compose --env-file "$(CURDIR)/.env" -f "$(CURDIR)/docker-compose.yml"

.PHONY: config prepare-data check up init start stop down
config:
	mkdir -p configs/clickhouse01 configs/clickhouse02 configs/clickhouse03 configs/clickhouse04
	REPLICA=01 SHARD=01 envsubst '$${REPLICA} $${SHARD}' < config.xml > configs/clickhouse01/config.xml
	REPLICA=02 SHARD=01 envsubst '$${REPLICA} $${SHARD}' < config.xml > configs/clickhouse02/config.xml
	REPLICA=03 SHARD=02 envsubst '$${REPLICA} $${SHARD}' < config.xml > configs/clickhouse03/config.xml
	REPLICA=04 SHARD=02 envsubst '$${REPLICA} $${SHARD}' < config.xml > configs/clickhouse04/config.xml
	cp users.xml configs/clickhouse01/users.xml
	cp users.xml configs/clickhouse02/users.xml
	cp users.xml configs/clickhouse03/users.xml
	cp users.xml configs/clickhouse04/users.xml

prepare-data:
	mkdir -p data/clickhouse01 data/clickhouse02 data/clickhouse03 data/clickhouse04 data/zookeeper/data data/zookeeper/datalog

check:
	$(COMPOSE) config --quiet

up: prepare-data check
	$(COMPOSE) up -d

init: prepare-data check
	$(COMPOSE) run --rm clickhouse-init

start:
	$(COMPOSE) start

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down
