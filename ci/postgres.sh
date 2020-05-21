#! /bin/bash
docker build -t postgres -f postgres/Dockerfile .

docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
