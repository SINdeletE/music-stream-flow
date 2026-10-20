COMPOSE = docker compose --env-file "$(CURDIR)/.env" -f "$(CURDIR)/docker-compose.yaml"

.PHONY: check up start stop down

check:
	$(COMPOSE) config --quiet

up: check
	$(COMPOSE) up -d --build

start:
	$(COMPOSE) start

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down
