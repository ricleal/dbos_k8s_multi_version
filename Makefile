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
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  version $(VERSION) (from pyproject.toml)   replicas $(REPLICAS)"

.PHONY: infra
infra: ## Create the namespace, Secret, Postgres and the RBAC the app needs
	@kubectl get nodes >/dev/null || { echo "cluster unreachable"; exit 1; }
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/05-secret.yaml
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

.PHONY: kill_version
kill_version: ## Hard-kill every app container of VER, simulating node loss (VER=0.1.7)
	@test -n "$(VER)" || { echo "usage: make kill_version VER=<application version>"; exit 1; }
	@# Models losing the machine a pod ran on: the process dies at once, with no
	@# SIGTERM and no drain, and the pod object goes with it. That is the only
	@# way to leave a version with active work and no pods, which is what
	@# cancel_stranded_versions exists to find.
	@#
	@# Neither obvious alternative produces it:
	@#
	@#   * `kubectl delete pod --force --grace-period=0` removes the pod OBJECT
	@#     but never stops the process. The container keeps running, keeps
	@#     dequeuing, and finishes the work — measured here, a version with zero
	@#     pods ran 48 workflows to SUCCESS — so nothing is ever stranded. It is
	@#     the same ghost hazard the teardown comment in `reset` warns about.
	@#   * `kubectl exec -- kill -9 1` cannot work either: the kernel discards a
	@#     SIGKILL sent to PID 1 from inside its own PID namespace.
	@#
	@# So kill it from the node, through the CRI, which is outside that namespace.
	@# Only meaningful against pods that are already Terminating. On a live pod
	@# the kubelet restarts the container under the same pod name — and therefore
	@# the same executor id — which is the crash case DBOS's own startup recovery
	@# already handles.
	@for pod in $$($(KUBECTL) get pods -l app=dbos-poc,version=$(VER) -o name | cut -d/ -f2); do \
	  ids=$$(docker exec $(NODE) crictl ps -q --label io.kubernetes.pod.name=$$pod); \
	  if [ -n "$$ids" ]; then \
	    docker exec $(NODE) crictl stop --timeout 0 $$ids >/dev/null && echo "killed $$pod"; \
	  fi; \
	done
	@$(KUBECTL) wait --for=delete pod -l app=dbos-poc,version=$(VER) --timeout=90s

.PHONY: dbos_reset
dbos_reset: ## Drop the DBOS system database, running the CLI in a live app pod
	@# `-c app`: the pod also has an init container, so without this kubectl
	@# picks one itself and says so ("Defaulted container ... out of ...").
	@#
	@# Source the same /app/.env the app reads (projected from the Secret), so
	@# the connection string is never repeated here.
	@#
	@# --sys-db-url is not optional even with the variable exported: outside
	@# DBOS Cloud the CLI never consults DBOS_SYSTEM_DATABASE_URL, and without
	@# the flag it exits "Missing database URL" — or, should a dbos-config.yaml
	@# ever appear without URLs in it, quietly resets a local SQLite file.
	@#
	@# Needs a running pod, and fails saying so if there is none.
	$(KUBECTL) exec deployment/dbos-poc -c app -- sh -c \
	  'set -a; . /app/.env; set +a; \
	   uv run dbos reset --yes --sys-db-url "$$DBOS_SYSTEM_DATABASE_URL"'

.PHONY: reset
reset: dbos_reset ## Reset the DBOS system database, then remove the app
	@# Order matters: dbos_reset needs a live pod to run in. The drop uses
	@# WITH (FORCE) and takes the database out from under the running pods, so
	@# they have to go straight after. Deleting the Deployment (rather than
	@# scaling it) also drops the old ReplicaSets, so nothing lingers from a
	@# previous version. Postgres and its volume are untouched.
	-$(KUBECTL) delete deployment dbos-poc --ignore-not-found --wait=false
	@# A short grace period, not --force. Two failure modes to thread between:
	@#
	@#   * the pod's own 1500s grace: on SIGTERM the app drains, and with the
	@#     database dropped it just polls a database that no longer exists until
	@#     the grace expires. (`--now` on the Deployment does not help — that
	@#     sets grace on that object, not on the pods.)
	@#   * --force --grace-period=0: removes the pod object without waiting for
	@#     the kubelet to kill anything, so the process can outlive it. Those
	@#     ghosts keep their queue pollers running, survive the drop on DBOS's
	@#     retries, and start dequeuing again the moment a new pod recreates the
	@#     database — invisible to the API, but stamping their executor id on
	@#     fresh work.
	@#
	@# 5 seconds gives SIGTERM time to arrive and guarantees a SIGKILL behind it.
	-$(KUBECTL) delete pod -l app=dbos-poc --grace-period=5 --ignore-not-found
	-$(KUBECTL) wait --for=delete pod -l app=dbos-poc --timeout=120s

.PHONY: clean
clean: ## Delete everything, including the Postgres volume
	-kubectl delete namespace $(NS) --wait=false
	-rm -rf $(RENDER_DIR)
