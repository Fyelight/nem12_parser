docker run -d \
  --name pg-nem12-db \
  -p 5432:5432 \
  -e POSTGRES_DB=nem12_db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=secret \
  -v pgdata_nem12:/var/lib/postgresql \
  postgres:18-alpine \
  -c shared_buffers=1GB \
  -c work_mem=64MB \
  -c max_wal_size=4GB \
  -c synchronous_commit=off
