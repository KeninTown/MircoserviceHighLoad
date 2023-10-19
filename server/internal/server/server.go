package server

import (
	"HighLoadServer/internal/entities"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"golang.org/x/exp/slog"
)

type Server struct {
	router   *gin.Engine
	producer sarama.AsyncProducer
}

func New(kafkaHost string) (*Server, error) {
	producer, err := sarama.NewAsyncProducer([]string{kafkaHost}, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %s", err.Error())
	}
	return &Server{
		router:   gin.Default(),
		producer: producer,
	}, nil
}

func (s *Server) Run(port string) {
	defer s.producer.Close()

	s.router.GET("/logo", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, "something goes wrong")
	})

	s.router.POST("/patient", func(ctx *gin.Context) {

		var patient entities.Patient

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
			Topic: "test",
			Value: sarama.StringEncoder(data),
		}

		s.producer.Input() <- msg

		ctx.JSON(http.StatusCreated, &patient)
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
