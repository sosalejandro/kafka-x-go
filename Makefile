up-kafka:
	docker compose -f ./kafka-docker-compose.yaml up -d

down-kafka:
	docker compose -f ./kafka-docker-compose.yaml down

up-otel:
	docker compose -f ./docker-compose.yaml up -d

down-otel:
	docker compose -f ./docker-compose.yaml down