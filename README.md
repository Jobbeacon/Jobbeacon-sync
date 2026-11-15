# JobBeacon sync

Service riêng đồng bộ tin tuyển dụng từ Postgres sang index Elasticsearch `jobs`. Nó đọc WAL của `job_search_outbox` bằng Debezium. API `Jobbeacon-be` không nhúng phần này.

`searchJobs` đọc index này. Chi tiết job vẫn đọc Postgres.

## Chạy

Postgres của JobBeacon phải bật `wal_level=logical`. Compose trong `infras-deployment` tạo database đó ở cổng `5433`.

```bash
cd ../infras-deployment
docker compose up -d jobbeacon-db elasticsearch
```

Chạy API trước để Liquibase tạo `job_search_outbox`, rồi chạy sync:

```bash
DB_URL=jdbc:postgresql://localhost:5433/job_platform \
DB_USERNAME=postgres \
DB_PASSWORD=postgres \
ELASTICSEARCH_URL=http://localhost:9200 \
mvn spring-boot:run
```

Health: `http://localhost:8081/jobbeacon-sync/health` (context path `/jobbeacon-sync`).

Service đăng ký vào discovery server (`EUREKA_URL`, mặc định `http://localhost:8761/eureka/`) với tên `jobbeacon-sync`. Nó không gọi HTTP sang service khác; đặt `EUREKA_ENABLED=false` để chạy không cần discovery.

Job đã có trong database được đổ vào outbox bằng cách chạy API một lần với `SEARCH_BACKFILL_ON_STARTUP=true`.

Dựng lại index từ các hàng outbox còn lại, rồi đổi alias `jobs`:

```bash
SYNC_MODE=rebuild mvn spring-boot:run
```

Chỉ chạy một instance. Một replication slot `jobbeacon_search` chỉ có một consumer.
