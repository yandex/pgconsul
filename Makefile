.PHONY: clean all

PG_MAJOR=14

PGCONSUL_IMAGE=pgconsul:behave
PROJECT=pgconsul
ZK_VERSION=3.7.1
export ZK_VERSION
INSTALL_DIR=$(DESTDIR)opt/yandex/pgconsul
REPLICATION_TYPE=quorum

clean_report:
	rm -rf htmlcov

clean: clean_report
	rm -rf ../yamail-pgconsul_*.build ../yamail-pgconsul_*.changes ../yamail-pgconsul_*.deb Dockerfile* docker/zookeeper/zookeeper-*.tar.gz test_ssh_key*
	mv --force static/pgconsul.sudoers.d.orig static/pgconsul.sudoers.d 2>/dev/null || true
	mv --force static/pgconsul.init.d.orig static/pgconsul.init.d 2>/dev/null || true
	rm -rf .tox __pycache__ pgconsul.egg-info .mypy_cache
	rm -rf junit_report

install:
	echo "Installing into $(INSTALL_DIR)"
	# Create installation directories
	mkdir -p $(DESTDIR)/opt/yandex
	mkdir -p $(DESTDIR)/usr/local/bin
	mkdir -p $(DESTDIR)/etc/pgconsul/plugins
	# Make venv
	python3 -m venv $(INSTALL_DIR)
# 	echo `git rev-list HEAD --count`-`git rev-parse --short HEAD` > $(INSTALL_DIR)/package.release
	echo "1-0303030" > $(INSTALL_DIR)/package.release
	# Install dependencies and pgconsul as python packages in venv
	$(INSTALL_DIR)/bin/pip install wheel
	$(INSTALL_DIR)/bin/pip install --pre -r requirements.txt
	$(INSTALL_DIR)/bin/pip install --pre .
	# Deliver pgconsul static files
	make -C static install
	mkdir -p $(DESTDIR)/etc/pgconsul/plugins
	# Fix "ValueError: bad marshal data (unknown type code)"
	find $(INSTALL_DIR) -name __pycache__ -type d -exec rm -rf {} +
	# Make symlinks in /usr/local/bin
	ln -s /opt/yandex/pgconsul/bin/pgconsul $(DESTDIR)/usr/local/bin
	ln -s /opt/yandex/pgconsul/bin/pgconsul-util $(DESTDIR)/usr/local/bin
	# Replace redundant paths with actual ones
	# E.g. /tmp/build/opt/yandex/pgconsul -> /opt/yandex/pgconsul
	test -n '$(DESTDIR)' \
		&& grep -l -r -F '$(INSTALL_DIR)' $(INSTALL_DIR) \
		| xargs sed -i -e 's|$(INSTALL_DIR)|/opt/yandex/pgconsul|' \
		|| true

build:
	cp -f docker/base/Dockerfile .
	yes | ssh-keygen -m PEM -t rsa -N '' -f test_ssh_key -C jepsen || true
	wget https://mirror.yandex.ru/mirrors/apache/zookeeper/zookeeper-$(ZK_VERSION)/apache-zookeeper-$(ZK_VERSION)-bin.tar.gz -nc -O docker/zookeeper/zookeeper-$(ZK_VERSION).tar.gz || true
	docker compose -p $(PROJECT) down --rmi all --remove-orphans
	docker compose -p $(PROJECT) -f jepsen-compose.yml down --rmi all --remove-orphans
	docker build -t pgconsulbase:latest . --label pgconsul_tests
	docker compose -p $(PROJECT) build --build-arg replication_type=$(REPLICATION_TYPE) --build-arg pg_major=$(PG_MAJOR)

build_package:
	docker build -f ./docker/dpkg/Dockerfile . --tag pgconsul_package_build:1.0 && docker run -e VERSION=$(BUILD_VERSION) -e BUILD_NUMBER=$(BUILD_NUM) pgconsul_package_build:1.0

build_pgconsul:
	rm -rf logs/
	cp -f tests/Dockerfile ./Dockerfile_pgconsul_behave
	docker build -t $(PGCONSUL_IMAGE) \
		--build-arg pg_major=$(PG_MAJOR) \
		-f ./Dockerfile_pgconsul_behave . \
		--label pgconsul_tests

jepsen_test:
	docker compose -p $(PROJECT) -f jepsen-compose.yml up -d
	docker exec pgconsul_postgresql1_1 /usr/local/bin/generate_certs.sh
	docker exec pgconsul_postgresql2_1 /usr/local/bin/generate_certs.sh
	docker exec pgconsul_postgresql3_1 /usr/local/bin/generate_certs.sh
	docker exec pgconsul_zookeeper1_1 bash -c '/usr/local/bin/generate_certs.sh && supervisorctl restart zookeeper'
	docker exec pgconsul_zookeeper2_1 bash -c '/usr/local/bin/generate_certs.sh && supervisorctl restart zookeeper'
	docker exec pgconsul_zookeeper3_1 bash -c '/usr/local/bin/generate_certs.sh && supervisorctl restart zookeeper'
	docker exec pgconsul_postgresql1_1 chmod +x /usr/local/bin/setup.sh
	docker exec pgconsul_postgresql2_1 chmod +x /usr/local/bin/setup.sh
	docker exec pgconsul_postgresql3_1 chmod +x /usr/local/bin/setup.sh
	timeout 600 docker exec pgconsul_postgresql1_1 /usr/local/bin/setup.sh $(PG_MAJOR)
	timeout 600 docker exec pgconsul_postgresql2_1 /usr/local/bin/setup.sh $(PG_MAJOR) pgconsul_postgresql1_1.pgconsul_pgconsul_net
	timeout 600 docker exec pgconsul_postgresql3_1 /usr/local/bin/setup.sh $(PG_MAJOR) pgconsul_postgresql1_1.pgconsul_pgconsul_net
	mkdir -p logs
	docker exec pgconsul_jepsen_1 chmod +x /root/jepsen/run.sh
	(docker exec pgconsul_jepsen_1 /root/jepsen/run.sh >logs/jepsen.log 2>&1 && tail -n 4 logs/jepsen.log && ./docker/jepsen/save_logs.sh $PG_MAJOR) || (./docker/jepsen/save_logs.sh $PG_MAJOR && tail -n 18 logs/jepsen.log && exit 1)
	docker compose -p $(PROJECT) -f jepsen-compose.yml down --rmi all

check_test: build_pgconsul
	PROJECT=$(PROJECT) \
	PGCONSUL_IMAGE=$(PGCONSUL_IMAGE) \
	PG_MAJOR=$(PG_MAJOR) \
	tox -e behave -- $(TEST_ARGS)

check_test_unstoppable: build_pgconsul
	PROJECT=$(PROJECT) \
	PGCONSUL_IMAGE=$(PGCONSUL_IMAGE) \
	PG_MAJOR=$(PG_MAJOR) \
	tox -e behave_unstoppable -- $(TEST_ARGS)

lint:
	tox -e yapf,flake8,pylint,bandit

jepsen: build jepsen_test

check: build check_test

check_unstoppable: build check_test_unstoppable

check-world: clean build check_test jepsen_test
