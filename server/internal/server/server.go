package server

import (
	"HighLoadServer/internal/server/handlers"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
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

var responseChannels sync.Map

// var responseChannels map[string]chan *sarama.ConsumerMessage
var mu sync.Mutex

func (s *Server) Run(port string) {
	defer s.producer.Close()
	defer s.consumer.Close()

	patientInfoConsumer, err := s.consumer.ConsumePartition("patientInfo", 0, sarama.OffsetNewest)
	if err != nil {
		slog.Error("failed to consume patient partition", "err", err)
	}
	defer patientInfoConsumer.Close()

	go func() {
		for {
			select {
			case msg, ok := <-patientInfoConsumer.Messages():
				if !ok {
					slog.Info("channel closed, exiting gorutine")
					return
				}
				chanId := string(msg.Key)

				mu.Lock()
				defer mu.Unlock()

				ch, ok := responseChannels.Load(chanId)
				if ok {
					ch.(chan *sarama.ConsumerMessage) <- msg
					responseChannels.Delete(chanId)
				}
			}
		}
	}()

	//configurate handlers
	h := handlers.New(s.producer, &responseChannels)

	//get patient info
	s.router.GET("/patients/:id", h.GetPatient())

	//create patient
	s.router.POST("/patients", h.CreatePatient())

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
