package server

import (
	"HighLoadServer/internal/entities"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"golang.org/x/exp/slog"
)

type Server struct {
	router   *gin.Engine
	producer sarama.AsyncProducer
	consumer sarama.Consumer
}

func New(kafkaHost string) (*Server, error) {
	op := "server.New()"
	producer, err := sarama.NewAsyncProducer([]string{kafkaHost}, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create kafka producer: %s", op, err.Error())
	}

	consumer, err := sarama.NewConsumer([]string{kafkaHost}, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create kafka consumer: %s", op, err.Error())
	}

	return &Server{
		router:   gin.Default(),
		producer: producer,
		consumer: consumer,
	}, nil
}

var responseChannels map[string]chan *sarama.ConsumerMessage
var patientsIdChannels map[string]chan *sarama.ConsumerMessage
var mu sync.Mutex

func (s *Server) Run(port string) {
	defer s.producer.Close()
	defer s.consumer.Close()

	responseChannels = make(map[string]chan *sarama.ConsumerMessage)
	patientsIdChannels = make(map[string]chan *sarama.ConsumerMessage)

	getConsumer, err := s.consumer.ConsumePartition("getPatient", 0, sarama.OffsetNewest)
	if err != nil {
		slog.Error("failed to consume patient partition", "err", err)
	}
	defer getConsumer.Close()

	createConsumer, err := s.consumer.ConsumePartition("createPatient", 0, sarama.OffsetNewest)
	if err != nil {
		slog.Error("failed to consume patient partition", "err", err)
	}
	defer createConsumer.Close()

	go func() {
		for {
			select {
			case msg, ok := <-createConsumer.Messages():
				if !ok {
					slog.Info("channel closed, exiting gorutine")
					return
				}
				chanId := string(msg.Key)

				mu.Lock()
				defer mu.Unlock()

				ch, ok := responseChannels[chanId]
				if ok {
					ch <- msg
					delete(responseChannels, chanId)
				}

			case msg, ok := <-createConsumer.Messages():
				if !ok {
					slog.Info("channel closed, exiting gorutine")
					return
				}

				chanId := string(msg.Key)

				mu.Lock()
				defer mu.Unlock()

				ch, ok := patientsIdChannels[chanId]
				if ok {
					ch <- msg
					delete(patientsIdChannels, chanId)
				}
			}
		}
	}()

	s.router.GET("/patient/:id", func(ctx *gin.Context) {
		requestId := uuid.New().String()

		idStr := ctx.Param("id")

		if id, err := strconv.Atoi(idStr); err != nil || id <= 0 {
			ctx.AbortWithStatusJSON(400, gin.H{"err": "invalid patient id"})
			return
		}

		msg := &sarama.ProducerMessage{
			Topic: "patients",
			Key:   sarama.StringEncoder(requestId),
			Value: sarama.StringEncoder(idStr),
		}

		s.producer.Input() <- msg

		responseCh := make(chan *sarama.ConsumerMessage)

		responseChannels[requestId] = responseCh

		select {
		case msg := <-responseCh:
			//TODO: delete json
			var patient entities.Patient
			if err := json.Unmarshal(msg.Value, &patient); err != nil {
				slog.Error("fail to decode request body", slog.String("err", err.Error()))
				ctx.JSON(http.StatusBadRequest, "failed to unmarshal json from kafka broker")
				return
			}
			ctx.JSON(200, patient)
			return

		case <-time.After(time.Second * 10):
			ctx.JSON(400, gin.H{"err": "failed to get patient info from db"})
			return
		}
	})

	//create patient
	s.router.POST("/patient", func(ctx *gin.Context) {
		var patient entities.Patient
		requestId := uuid.New().String()

		if err := ctx.ShouldBindJSON(&patient); err != nil {
			slog.Error("fail to decode request body", slog.String("err", err.Error()))
			ctx.JSON(http.StatusBadRequest, "something goes wrong")
			return
		}

		//Todo: send data to kafka and return response to client
		data, err := json.Marshal(&patient)
		if err != nil {
			slog.Error("failed to marshal patient", slog.String("err", err.Error()))
		}

		msg := &sarama.ProducerMessage{
			Topic: "createPatient",
			Key:   sarama.StringEncoder(requestId),
			Value: sarama.StringEncoder(data),
		}

		s.producer.Input() <- msg

		responseCh := make(chan *sarama.ConsumerMessage)

		patientsIdChannels[requestId] = responseCh

		select {
		case msg := <-responseCh:
			//TODO: delete json
			var patient entities.Patient
			if err := json.Unmarshal(msg.Value, &patient); err != nil {
				slog.Error("fail to decode request body", slog.String("err", err.Error()))
				ctx.JSON(http.StatusBadRequest, "failed to unmarshal json from kafka broker")
				return
			}
			ctx.JSON(201, patient)
			return
		case <-time.After(time.Second * 10):
			ctx.JSON(400, gin.H{"err": "failed to create patient"})
			return
		}
	})

	serv := http.Server{
		Addr:    port,
		Handler: s.router,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := serv.ListenAndServe(); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	}()

	slog.Info("server is listening", slog.String("port", port))

	<-ctx.Done()

	slog.Info("start to finish server gracefully...")

	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := serv.Shutdown(ctxTimeout); err != nil {
		slog.Error("failed to shutdown server gracefully")
	}

	slog.Info("finished server gracefully")
}
