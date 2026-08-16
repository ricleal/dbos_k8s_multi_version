SHELL := /bin/bash

# The application version comes from pyproject.toml and nowhere else, so it
# cannot disagree with the code in the image. `make bump` cuts a new one.
#
# `:=`, never `?=`: `?=` yields to the environment, and .envrc runs
# `dotenv_if_exists .env`, so a stray shell variable would silently win.
VERSION  := $(shell python3 -c "import tomllib;print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
NS       := dbos-poc
REPLICAS := 3
IMAGE    := dbos-poc:$(VERSION)

# Docker Desktop runs Kubernetes on a kind-style node with its own image store.
# The node container is hidden from `docker ps`, but `docker exec` reaches it.
NODE     := desktop-control-plane

RENDER_DIR := .rendered
MANIFEST   := $(RENDER_DIR)/app-$(VERSION).yaml
KUBECTL    := kubectl -n $(NS)
PSQL       := $(KUBECTL) exec -i postgres-0 -- psql -U dbos -d dbos_poc -c

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  version $(VERSION) (from pyproject.toml)   replicas $(REPLICAS)"

.PHONY: infra
infra: ## Create the namespace, Postgres and the RBAC the app needs
	@kubectl get nodes >/dev/null || { echo "cluster unreachable"; exit 1; }
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/10-postgres.yaml
	kubectl apply -f k8s/20-rbac.yaml
	$(KUBECTL) rollout status statefulset/postgres --timeout=180s

.PHONY: build
build: ## Build and load $(IMAGE) into the node's image store
	docker build -t $(IMAGE) .
	docker save $(IMAGE) | docker exec -i $(NODE) ctr -n k8s.io images import -

.PHONY: deploy
deploy: ## Roll out the current version (build first)
	@echo ">> image=$(IMAGE) application_version=$(VERSION) replicas=$(REPLICAS)"
	@mkdir -p $(RENDER_DIR)
	@sed -e 's|__IMAGE__|$(IMAGE)|g' -e 's|__VERSION__|$(VERSION)|g' \
	     -e 's|__REPLICAS__|$(REPLICAS)|g' k8s/30-app.yaml > $(MANIFEST)
	kubectl apply -f $(MANIFEST)
	@echo ">> old pods stay up until they drain; watch with 'make status'"

.PHONY: bump
bump: ## Cut a new application version
	@uv version --bump patch
	@echo "project.version is now $$($(MAKE) --no-print-directory version)"

.PHONY: version
version: ## Print the project version, which is the DBOS application version
	@echo $(VERSION)

.PHONY: status
status: ## Pods by version, and the dbos schema's own view of the work
	@$(KUBECTL) get pods -L version
	@echo ""
	@$(PSQL) "SELECT application_version AS version, status, count(*) \
	          FROM dbos.workflow_status GROUP BY 1,2 ORDER BY 1,2;" 2>/dev/null \
	  || echo "(no database yet)"

.PHONY: logs
logs: ## Follow every app pod's logs
	$(KUBECTL) logs -l app=dbos-poc --all-containers --tail=50 -f --max-log-requests=10

.PHONY: reset
reset: ## Delete the app and drop the DBOS schema, keeping Postgres
	@# Deleting the Deployment (rather than scaling it) also drops the old
	@# ReplicaSets, so nothing lingers from a previous version.
	-$(KUBECTL) delete deployment dbos-poc --ignore-not-found --now
	-$(KUBECTL) wait --for=delete pod -l app=dbos-poc --timeout=300s
	@# Tolerated, not silenced: before the first deploy the database does not
	@# exist yet (DBOS creates it at launch), and psql exits non-zero saying so.
	-$(PSQL) "DROP SCHEMA IF EXISTS dbos CASCADE;"

.PHONY: clean
clean: ## Delete everything, including the Postgres volume
	-kubectl delete namespace $(NS) --wait=false
	-rm -rf $(RENDER_DIR)
