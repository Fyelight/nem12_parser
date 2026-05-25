cat ../sql_chunks/*.sql | docker exec -i pg-nem12-db psql -U postgres -d nem12_db
