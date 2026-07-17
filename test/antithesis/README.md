## Usage

### Local Testing

Only pre-req is docker

```
# provision orioledb configured with s3 storage, jepsen workload
config/make-docker-compose setup/s3 workload/jepsen-repeatable-read | docker compose -f - up -d

# teardown
config/make-docker-compose setup/s3 workload/jepsen-repeatable-read | docker compose -f - down
```
