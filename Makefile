SHELL := /bin/sh

COMPOSE ?= docker compose
UV ?= uv
APP_SERVICE ?= stream-processor
BROKER_SERVICE ?= kafka-1

LOG_LEVEL ?= INFO
MAX_MESSAGES ?= 2
WAIT_SECONDS ?= 3

OWNER_ID ?= alice
BLOCKED_USER_ID ?= bob
WORD ?= kafka
SENDER_ID ?= carol
RECIPIENT_ID ?= alice
MESSAGE ?= Kafka полезна

.DEFAULT_GOAL := help

.PHONY: help install lint format-check format pre-commit check \
	pre-commit-install \
	infra-up init compose-up up compose-down down compose-clean clean \
	ps logs restart broker-check topics local-worker worker \
	block-user unblock-user ban-word allow-word send-message \
	docker-block-user docker-unblock-user docker-ban-word docker-allow-word docker-send-message \
	consume-filtered scenario-local scenario-docker

help: ## Показать доступные команды
	@awk 'BEGIN {FS = ":.*## "; printf "Доступные команды:\n\n"} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Установить зависимости проекта и dev-зависимости
	$(UV) sync --all-groups

lint: ## Проверить код линтером Ruff
	$(UV) run ruff check .

format-check: ## Проверить форматирование Ruff без изменения файлов
	$(UV) run ruff format --check .

format: ## Отформатировать код Ruff
	$(UV) run ruff format .

pre-commit: ## Запустить все pre-commit hook-и
	$(UV) run pre-commit run --all-files

pre-commit-install: ## Установить pre-commit hook-и в локальный git checkout
	$(UV) run pre-commit install

check: lint format-check ## Выполнить основные проверки качества кода

infra-up: ## Запустить Kafka и Kafka UI, затем создать прикладные топики
	$(COMPOSE) up -d kafka-1 kafka-2 kafka-3 kafka-ui
	$(COMPOSE) run --rm kafka-init

init: infra-up ## Алиас для запуска Kafka и инициализации топиков

compose-up: ## Собрать образ и запустить весь стек в Docker Compose
	$(COMPOSE) up -d --build

up: compose-up ## Алиас для запуска всего стека в Docker Compose

compose-down: ## Остановить Compose-сервисы, сохранив volumes
	$(COMPOSE) down

down: compose-down ## Алиас для остановки Compose-сервисов

compose-clean: ## Остановить Compose и удалить volumes Kafka
	$(COMPOSE) down -v

clean: compose-clean ## Алиас для полной очистки состояния Kafka

ps: ## Показать состояние Compose-сервисов
	$(COMPOSE) ps --all

logs: ## Следить за логами stream processor-а
	$(COMPOSE) logs -f $(APP_SERVICE)

restart: ## Перезапустить stream processor
	$(COMPOSE) restart $(APP_SERVICE)

broker-check: ## Проверить доступность Kafka-брокера
	$(COMPOSE) exec $(BROKER_SERVICE) kafka-broker-api-versions --bootstrap-server $(BROKER_SERVICE):9092

topics: ## Описать прикладные Kafka-топики
	$(COMPOSE) exec $(BROKER_SERVICE) kafka-topics --bootstrap-server $(BROKER_SERVICE):9092 --describe --topic messages
	$(COMPOSE) exec $(BROKER_SERVICE) kafka-topics --bootstrap-server $(BROKER_SERVICE):9092 --describe --topic filtered_messages
	$(COMPOSE) exec $(BROKER_SERVICE) kafka-topics --bootstrap-server $(BROKER_SERVICE):9092 --describe --topic blocked_users
	$(COMPOSE) exec $(BROKER_SERVICE) kafka-topics --bootstrap-server $(BROKER_SERVICE):9092 --describe --topic banned_words

local-worker: ## Запустить Faust worker локально через uv
	$(UV) run kafka-app worker --log-level $(LOG_LEVEL)

worker: local-worker ## Алиас для локального worker-а

block-user: ## Заблокировать отправителя локально (OWNER_ID=alice BLOCKED_USER_ID=bob)
	$(UV) run kafka-app block-user "$(OWNER_ID)" "$(BLOCKED_USER_ID)"

unblock-user: ## Разблокировать отправителя локально
	$(UV) run kafka-app unblock-user "$(OWNER_ID)" "$(BLOCKED_USER_ID)"

ban-word: ## Добавить слово в цензуру локально (WORD=kafka)
	$(UV) run kafka-app ban-word "$(WORD)"

allow-word: ## Убрать слово из цензуры локально
	$(UV) run kafka-app allow-word "$(WORD)"

send-message: ## Отправить сообщение локально (SENDER_ID=... RECIPIENT_ID=... MESSAGE=...)
	$(UV) run kafka-app send-message "$(SENDER_ID)" "$(RECIPIENT_ID)" "$(MESSAGE)"

docker-block-user: ## Заблокировать отправителя через контейнер приложения
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app block-user "$(OWNER_ID)" "$(BLOCKED_USER_ID)"

docker-unblock-user: ## Разблокировать отправителя через контейнер приложения
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app unblock-user "$(OWNER_ID)" "$(BLOCKED_USER_ID)"

docker-ban-word: ## Добавить слово в цензуру через контейнер приложения
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app ban-word "$(WORD)"

docker-allow-word: ## Убрать слово из цензуры через контейнер приложения
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app allow-word "$(WORD)"

docker-send-message: ## Отправить сообщение через контейнер приложения
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app send-message "$(SENDER_ID)" "$(RECIPIENT_ID)" "$(MESSAGE)"

consume-filtered: ## Прочитать результаты из filtered_messages
	$(COMPOSE) exec -T $(BROKER_SERVICE) kafka-console-consumer \
		--bootstrap-server $(BROKER_SERVICE):9092 \
		--topic filtered_messages \
		--from-beginning \
		--max-messages $(MAX_MESSAGES) \
		--formatter-property print.key=true \
		--formatter-property 'key.separator= | '

scenario-local: ## Запустить демонстрационный сценарий через локальный CLI
	$(UV) run kafka-app block-user alice bob
	$(UV) run kafka-app ban-word kafka
	@sleep $(WAIT_SECONDS)
	$(UV) run kafka-app send-message bob alice "Сообщение от заблокированного пользователя"
	$(UV) run kafka-app send-message carol alice "Kafka полезна"
	$(UV) run kafka-app send-message dave alice "kafkagram остаётся без изменений"

scenario-docker: ## Запустить демонстрационный сценарий через контейнер приложения
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app block-user alice bob
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app ban-word kafka
	@sleep $(WAIT_SECONDS)
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app send-message bob alice "Сообщение от заблокированного пользователя"
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app send-message carol alice "Kafka полезна"
	$(COMPOSE) exec $(APP_SERVICE) uv run --frozen --no-dev --no-sync kafka-app send-message dave alice "kafkagram остаётся без изменений"
