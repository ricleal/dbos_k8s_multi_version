SHELL := /bin/bash

# Two versions, and the difference between them decides what a deploy does.
#
# VERSION is the release: project.version from pyproject.toml, and the image tag.
# DBOS_VERSION is the compatibility boundary: the MAJOR component only, as
# v<major>, matching poc/config.py dbos_version(). It names the Deployment.
#
#   0.1.2 -> 0.1.3   both v0   same Deployment re-applied -> rolling update
#   0.1.3 -> 0.2.0   both v0   same Deployment re-applied -> rolling update
#   0.2.0 -> 1.0.0   v0 -> v1  a NEW Deployment beside the old one
#
# `:=`, never `?=`: `?=` yields to the environment, and .envrc runs
# `dotenv_if_exists .env`, so a stray shell variable would silently win.
VERSION      := $(shell python3 -c "import tomllib;print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
DBOS_VERSION := v$(word 1,$(subst ., ,$(VERSION)))
NS           := dbos-poc
REPLICAS     := 3
IMAGE        := dbos-poc:$(VERSION)

# Docker Desktop runs Kubernetes on a kind-style node with its own image store.
# The node container is hidden from `docker ps`, but `docker exec` reaches it.
NODE     := desktop-control-plane

# One Deployment for each DBOS version, so this name is stable across releases
# of the same major. Already DNS-1123: v0, v1.
DEPLOY_NAME := dbos-poc-$(DBOS_VERSION)

RENDER_DIR := .rendered
MANIFEST   := $(RENDER_DIR)/app-$(VERSION).yaml
KUBECTL    := kubectl -n $(NS)

# Hard limit on how long two DBOS versions may coexist, in seconds. 24 hours.
# Past it, `make retire` deletes the old fleet whether or not it has drained,
# which CAN DESTROY WORK — see the deadline notes on that target. Set 0 to
# disable it and let a version live until it is empty.
RETIRE_MAX_AGE_SEC := 86400

# Run a Python one-liner inside any app pod. Used to reach the Service from
# inside the cluster without pulling a second image.
define in_pod
$(KUBECTL) exec $$($(KUBECTL) get pod -l app=dbos-poc -o name | head -1) -c app -- \
  /app/.venv/bin/python -c "from urllib.request import urlopen, Request; $(1)"
endef
PSQL       := $(KUBECTL) exec -i postgres-0 -- psql -U dbos -d dbos_poc -c
# The same database, machine-readable: one record per line, fields separated by
# `|`, no header and no padding. For reading in a shell loop.
PSQL_ROWS  := $(KUBECTL) exec -i postgres-0 -- psql -U dbos -d dbos_poc -qAt -F'|' -c

# The whole retirement decision, as one query. It returns one line for each
# version that MAY be considered:
#
#   version_name | active workflows | seconds since it stopped being latest
#
# Each guard is a clause, and the reason each is here is in the README under
# "How a version retires":
#
#   * Not the latest version. LEAD is NULL for the newest registered row, so
#     `superseded_at IS NOT NULL` drops it. A version cannot retire itself.
#   * Only versions DBOS has seen launch. The rows come from
#     dbos.application_versions, and a version registers there when its first
#     pod launches — several seconds after its Deployment is created. Reading
#     Deployments alone would find the incoming fleet with no work and retire it
#     on sight. Measured at 4 seconds after a deploy, before its pods were up.
#   * How much work it still owns. The same three statuses the in-process drain
#     counts, so "drained" means one thing in this repo.
#   * How long it has been superseded: now, less its successor's registration.
#     Not the Deployment's creationTimestamp, which is immutable and survives
#     every patch release, so it would report the age of the first deploy of
#     that major version — potentially months.
RETIRE_QUERY := WITH superseded AS ( \
    SELECT version_name, \
           LEAD(version_timestamp) OVER (ORDER BY version_timestamp) AS superseded_at \
      FROM dbos.application_versions \
  ), active AS ( \
    SELECT application_version AS version_name, count(*) AS n \
      FROM dbos.workflow_status \
     WHERE status IN ('PENDING','ENQUEUED','DELAYED') \
     GROUP BY 1 \
  ) \
  SELECT s.version_name, COALESCE(a.n, 0), \
         ((EXTRACT(EPOCH FROM now()) * 1000 - s.superseded_at) / 1000)::bigint \
    FROM superseded s LEFT JOIN active a USING (version_name) \
   WHERE s.superseded_at IS NOT NULL ORDER BY 1

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  release $(VERSION) -> DBOS version $(DBOS_VERSION) -> $(DEPLOY_NAME)   replicas $(REPLICAS)"
	@echo "  a major bump starts a new fleet; anything else is a rolling update"

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
deploy: ## Deploy $(VERSION): rolling update, or a new fleet on a major bump
	@# Which path this takes is not a choice made here — it follows from whether
	@# the Deployment already exists, which follows from the major version.
	@if $(KUBECTL) get deployment $(DEPLOY_NAME) >/dev/null 2>&1; then \
	  echo ">> ROLLING UPDATE of $(DEPLOY_NAME) to $(VERSION)"; \
	  echo ">> same DBOS version ($(DBOS_VERSION)): new pods may run the old pods' work,"; \
	  echo ">> so the old pods exit without draining and siblings adopt their rows"; \
	else \
	  echo ">> NEW FLEET $(DEPLOY_NAME) for DBOS version $(DBOS_VERSION), from $(VERSION)"; \
	  echo ">> any older fleet keeps running until its backlog is finished"; \
	fi
	@mkdir -p $(RENDER_DIR)
	@sed -e 's|__IMAGE__|$(IMAGE)|g' -e 's|__VERSION__|$(DBOS_VERSION)|g' \
	     -e 's|__CODE_VERSION__|$(VERSION)|g' \
	     -e 's|__DEPLOY_NAME__|$(DEPLOY_NAME)|g' \
	     -e 's|__REPLICAS__|$(REPLICAS)|g' k8s/30-app.yaml > $(MANIFEST)
	kubectl apply -f $(MANIFEST)
	$(KUBECTL) rollout status deployment/$(DEPLOY_NAME) --timeout=300s
	@# The cutover, once the new pods are Ready: one selector field, carrying the
	@# DBOS version. A rolling update leaves it unchanged; a major bump moves it,
	@# and older fleets leave the endpoint list while they carry on working.
	@sed -e 's|__VERSION__|$(DBOS_VERSION)|g' k8s/26-service.yaml > $(RENDER_DIR)/service.yaml
	kubectl apply -f $(RENDER_DIR)/service.yaml
	@echo ">> traffic now goes to DBOS version $(DBOS_VERSION) (code $(VERSION))"

.PHONY: retire
retire: ## Delete the fleet of every older version that has finished its work
	@# THE CRON ENTRY POINT, and the other half of a deploy.
	@#
	@# Deploying is a person's decision. Retiring is not: it is one question —
	@# "is that old fleet finished yet" — asked over and over until the answer is
	@# yes. A schedule asks it better than a person, and better than the app.
	@#
	@# It is deliberately outside the cluster. The app reads pods, because only
	@# the API server can say whether the executor that claimed a row still
	@# exists. It reads nothing else and it writes nothing at all. An application
	@# that can delete its own Deployment is a much larger blast radius than one
	@# that can list pods, and it buys nothing: the decision needs no DBOS
	@# runtime, only the system database and kubectl.
	@#
	@# THE PROCEDURE TO FOLLOW (this repo does not install it):
	@#
	@#   1. Check the repository out on a host that has kubectl, with a context
	@#      for this cluster and rights to delete deployments and
	@#      poddisruptionbudgets in the $(NS) namespace.
	@#   2. Add one crontab entry, every five minutes:
	@#
	@#        */5 * * * * cd /srv/dbos-poc && make retire >> /var/log/dbos-retire.log 2>&1
	@#
	@#   3. Nothing else. The host needs no database credentials: every read goes
	@#      through `kubectl exec postgres-0 -- psql`.
	@#
	@# Five minutes is a latency, not a risk. A fleet that has finished its
	@# backlog idles until the next run: it receives no traffic, it creates no
	@# work, and nothing waits on it. The only cost is the pods it holds.
	@#
	@# Safe to run at any time, by hand or from cron, and safe to run twice at
	@# once: deleting an object that another run already deleted is a no-op here.
	@#
	@# The delete takes both objects, selected by the version label. The budget
	@# is named after the Deployment but is not owned by it, so deleting the
	@# Deployment alone would leave one budget behind for every version ever
	@# deployed.
	@#
	@# A version whose fleet has already gone is passed over in silence. Every
	@# version ever deployed stays in the registry, so reporting those would grow
	@# the cron log without bound and bury the lines that matter. One header line
	@# for each run is enough to show that cron is alive.
	@#
	@# No comments inside the recipe below: make hands the whole
	@# backslash-continued block to the shell as ONE logical line, so a `#` would
	@# comment out everything after it.
	@rows=$$($(PSQL_ROWS) "$(RETIRE_QUERY)") || \
	  { echo ">> the system database did not answer; retiring nothing"; exit 1; }; \
	if [ -z "$$rows" ]; then \
	  echo ">> only one version has ever launched; nothing to retire"; exit 0; \
	fi; \
	echo ">> $$(echo "$$rows" | wc -l) superseded version(s) registered, deadline $(RETIRE_MAX_AGE_SEC)s"; \
	while IFS='|' read -r version active age; do \
	  test -n "$$version" || continue; \
	  if [ -z "$$($(KUBECTL) get deployment -l app=dbos-poc,version=$$version -o name)" ]; then \
	    continue; \
	  fi; \
	  if [ "$$active" -eq 0 ]; then \
	    echo ">> $$version: drained, retiring it (superseded $${age}s ago)"; \
	  elif [ "$(RETIRE_MAX_AGE_SEC)" -gt 0 ] && [ "$$age" -ge "$(RETIRE_MAX_AGE_SEC)" ]; then \
	    echo ">> $$version: FORCING retirement with $$active workflows still active,"; \
	    echo ">>   superseded $${age}s ago, past the $(RETIRE_MAX_AGE_SEC)s deadline."; \
	    echo ">>   Its pods drain on SIGTERM, so work that fits inside the drain"; \
	    echo ">>   budget still finishes. The rest is cancelled by the app, loudly."; \
	  else \
	    echo ">> $$version: keeping it, $$active workflows still active (superseded $${age}s ago)"; \
	    continue; \
	  fi; \
	  echo ">> deleting the fleet is what finally sends SIGTERM to its pods"; \
	  $(KUBECTL) delete deployment,poddisruptionbudget -l app=dbos-poc,version=$$version; \
	done <<< "$$rows"

.PHONY: bump
bump: ## Cut a new release. PART=patch (default), minor, or major
	@# Only PART=major starts a new DBOS version, and therefore a new fleet.
	@uv version --bump $(or $(PART),patch)
	@$(MAKE) --no-print-directory version

.PHONY: version
version: ## Print the release version and the DBOS version it maps to
	@echo "release $(VERSION)  ->  DBOS version $(DBOS_VERSION)  ->  deployment $(DEPLOY_NAME)"

.PHONY: status
status: ## Fleets, pods, the Service target, and the dbos schema's view of the work
	@echo "-- fleets (one Deployment per version) --"
	@$(KUBECTL) get deployments -l app=dbos-poc -L version
	@echo ""
	@echo "-- Service routes to version --"
	@$(KUBECTL) get service dbos-poc -o jsonpath='{.spec.selector.version}{"\n"}' 2>/dev/null \
	  || echo "(no Service yet)"
	@echo ""
	@$(KUBECTL) get pods -L version
	@echo ""
	@$(PSQL) "SELECT application_version AS version, status, count(*) \
	          FROM dbos.workflow_status GROUP BY 1,2 ORDER BY 1,2;" 2>/dev/null \
	  || echo "(no database yet)"

.PHONY: api
api: ## Call the Service and report which version answered
	@# Sent through the Service, so the reply is the selector's answer, not a
	@# pod's. Issued from inside an arbitrary app pod — which may well be an OLD
	@# one, and the reply still names the new version. That is the demonstration:
	@# an old pod is running, reachable and working, and receives no traffic.
	@#
	@# The app image already has an interpreter, so this pulls nothing.
	@$(call in_pod,print(urlopen('http://dbos-poc/version').read().decode()))

.PHONY: work
work: ## Ask the Service to start one parent workflow
	@$(call in_pod,print(urlopen(Request('http://dbos-poc/work',method='POST')).read().decode()))

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
	@# Any app pod of any version will do — they all share one system database.
	@# Selected by label rather than by Deployment name, which now varies.
	$(KUBECTL) exec $$($(KUBECTL) get pod -l app=dbos-poc -o name | head -1) -c app -- sh -c \
	  'set -a; . /app/.env; set +a; \
	   uv run dbos reset --yes --sys-db-url "$$DBOS_SYSTEM_DATABASE_URL"'

.PHONY: reset
reset: dbos_reset ## Reset the DBOS system database, then remove the app
	@# Order matters: dbos_reset needs a live pod to run in. The drop uses
	@# WITH (FORCE) and takes the database out from under the running pods, so
	@# they have to go straight after. Every version's fleet goes, selected by
	@# label rather than by name, along with each fleet's budget — retirement
	@# would normally remove both, but a reset does not wait for a drain.
	@# Postgres and its volume are untouched.
	-$(KUBECTL) delete deployment,poddisruptionbudget -l app=dbos-poc \
	  --ignore-not-found --wait=false
	@# A short grace period, not --force. Two failure modes to thread between:
	@#
	@#   * the pod's own grace period: on SIGTERM the app drains, and with the
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
