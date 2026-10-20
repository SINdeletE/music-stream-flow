COMPOSE = docker compose --env-file "$(CURDIR)/.env" -f "$(CURDIR)/docker-compose.yaml"

.PHONY: check up init start stop down

check:
	$(COMPOSE) config --quiet

up: check
	$(COMPOSE) up -d

init: check
	$(COMPOSE) run --rm kafka-init

start:
	$(COMPOSE) start

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down
