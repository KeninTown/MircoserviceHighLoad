FROM golang

WORKDIR /app/server

COPY ../ . 
RUN go mod download
RUN go build -o ./cmd/main.go

CMD ["./main"]