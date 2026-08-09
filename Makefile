IMAGE ?= airflow-workshop
CONTAINER ?= airflow-workshop
PORT ?= 8081
WS_PORT ?= 3001

.PHONY: help build run rebuild restart stop logs shell clean

help:
	@printf '%s\n' \
		'make build    Build the Docker image' \
		'make run      Run the container in the background' \
		'make rebuild  Stop, rebuild, and run the container' \
		'make restart  Restart the existing container' \
		'make stop     Stop and remove the container' \
		'make logs     Follow container logs' \
		'make shell    Open a shell in the running container' \
		'make clean    Remove the container and image'

build:
	docker build --tag $(IMAGE) .

run:
	docker run --detach \
		--rm \
		--name $(CONTAINER) \
		--init \
		--publish $(PORT):8080 \
		--publish $(WS_PORT):3001 \
		--volume "$(CURDIR)/content:/usr/src/app/content" \
		$(IMAGE)
	@printf 'Quartz is available at http://localhost:%s\n' '$(PORT)'

rebuild: stop build run

restart:
	docker restart $(CONTAINER)

stop:
	@if docker container inspect $(CONTAINER) >/dev/null 2>&1; then \
		docker rm --force $(CONTAINER); \
	else \
		printf 'Container %s is already stopped\n' '$(CONTAINER)'; \
	fi

logs:
	docker logs --follow $(CONTAINER)

shell:
	docker exec --interactive --tty $(CONTAINER) sh

clean: stop
	-docker image rm $(IMAGE)