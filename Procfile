build: while true; do make -qs bin/goalert || make bin/goalert || (echo '\033[0;31mBuild Failure'; sleep 3); sleep 0.1; done

@watch-file=./bin/goalert
goalert: ./bin/goalert -l=localhost:3030 --ui-dir=web/src/build --listen-prometheus=localhost:2112 --listen-pprof=localhost:6060 --pprof-mutex-profile-fraction=1 --pprof-block-profile-rate=1000 --enable-secure-headers

smtp: ./bin/tools/mailpit -s localhost:1025 -l localhost:8025 --allow-internal-http-requests
prom: bin/tools/prometheus --log.level=warn --config.file=devtools/prometheus/prometheus.yml --storage.tsdb.path=bin/prom-data/ --web.listen-address=localhost:9090

@watch-file=./web/src/esbuild.config.js
ui: ./bin/tools/bun run esbuild --watch

oidc: go tool mockoidc
