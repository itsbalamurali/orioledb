# syntax=docker/dockerfile:1

# BEGIN build image
FROM --platform=${BUILDPLATFORM} clojure:temurin-21-lein-2.11.2-bookworm-slim AS build

ARG JEPSEN_POSTGRES_REF=main

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/jepsen-postgres
RUN git init \
    && git remote add origin https://github.com/jepsen-io/postgres.git \
    && git fetch --depth=1 origin "${JEPSEN_POSTGRES_REF}" \
    && git checkout --detach FETCH_HEAD \
    && cd postgres \
    && lein uberjar \
    && cp target/*-standalone.jar /jepsen-postgres.jar

RUN jlink \
    --add-modules java.base,java.desktop,java.instrument,java.management,java.naming,java.scripting,java.security.jgss,java.sql,jdk.crypto.ec,jdk.random,jdk.unsupported \
    --no-header-files \
    --no-man-pages \
    --output /opt/java-runtime

# END build image

# BEGIN runtime

FROM --platform=${BUILDPLATFORM} debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      gnuplot-nox \
      git-core \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:${PATH}"

WORKDIR /opt/jepsen
COPY --from=build /opt/java-runtime /opt/java
COPY --from=build /jepsen-postgres.jar ./jepsen-postgres.jar

ENV JEPSEN_MAX_WRITES_PER_KEY=4 \
    JEPSEN_CONCURRENCY=50 \
    JEPSEN_RATE=500 \
    JEPSEN_WORKLOAD=append \
    JEPSEN_ISOLATION=serializable \
    JEPSEN_TIME_LIMIT=60 \
    JEPSEN_NEMESIS=none \
    JEPSEN_NODE=localhost \
    JEPSEN_POSTGRES_USER=jepsen \
    JEPSEN_POSTGRES_PASSWORD=pw \
    JEPSEN_POSTGRES_PORT=5432

COPY --chmod=755 <<'EOF' /opt/antithesis/test/v1/jepsen-postgres/serial_driver_jepsen-postgres
#!/bin/sh
set -eu

status=0
java \
    -Djava.awt.headless=true \
    -server \
    -jar /opt/jepsen/jepsen-postgres.jar \
    test-all \
    --no-ssh \
    --existing-postgres \
    --workload "$JEPSEN_WORKLOAD" \
    --isolation "$JEPSEN_ISOLATION" \
    --concurrency "$JEPSEN_CONCURRENCY" \
    --max-writes-per-key "$JEPSEN_MAX_WRITES_PER_KEY" \
    --rate "$JEPSEN_RATE" \
    --time-limit "$JEPSEN_TIME_LIMIT" \
    --nemesis "$JEPSEN_NEMESIS" \
    --node "$JEPSEN_NODE" \
    --postgres-user "$JEPSEN_POSTGRES_USER" \
    --postgres-password "$JEPSEN_POSTGRES_PASSWORD" \
    --postgres-port "$JEPSEN_POSTGRES_PORT" \
    "$@" || status=$?

if [ -n "${ANTITHESIS_OUTPUT_DIR:-}" ] && [ -d store ]; then
    cp -R store "$ANTITHESIS_OUTPUT_DIR/"
fi

exit "$status"
EOF

# entrypoint script
# Under antithesis => sleep inifinity.  Antithesis owns executing the test template script
# Local testing => execute the test template script
# 
COPY --chmod=755 <<'EOF' /usr/local/bin/jepsen-postgres-entrypoint
#!/bin/sh
set -eu

driver=/opt/antithesis/test/v1/jepsen-postgres/serial_driver_jepsen-postgres

if [ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]; then
    exec sleep infinity
fi

exec "$driver" "$@"
EOF

ENTRYPOINT ["/usr/local/bin/jepsen-postgres-entrypoint"]

# END runtime
