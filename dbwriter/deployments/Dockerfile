FROM golang

WORKDIR /app/dbwriter

COPY ../ .

#download psql
RUN apt-get update
RUN apt-get -y install postgresql-client

RUN chmod +x wait-for-postgres.sh

RUN go mod download
RUN go build -o ./cmd/main.go

CMD ["./wait-for-postgres.sh", "./main"]