FROM golang

WORKDIR /app/server

COPY . . 

RUN chmod +x wait-for-it.sh
RUN go mod download
RUN go build -o ./main ./cmd/main.go
