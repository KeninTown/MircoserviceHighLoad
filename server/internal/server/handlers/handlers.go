package handlers

import (
	"HighLoadServer/internal/entities"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Handler struct {
	producer     sarama.AsyncProducer
	responseChan *sync.Map
}

func New(producer sarama.AsyncProducer, respChan *sync.Map) Handler {
	return Handler{
		producer:     producer,
		responseChan: respChan,
	}
}

func (h Handler) GetPatient() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		requestId := uuid.New().String()

		idStr := ctx.Param("id")

		if id, err := strconv.Atoi(idStr); err != nil || id <= 0 {
			ctx.AbortWithStatusJSON(400, gin.H{"err": "invalid patient id"})
			return
		}

		msg := &sarama.ProducerMessage{
			Topic: "patientId",
			Key:   sarama.StringEncoder(requestId),
			Value: sarama.StringEncoder(idStr),
		}

		h.producer.Input() <- msg

		responseCh := make(chan *sarama.ConsumerMessage)

		h.responseChan.Store(requestId, responseCh)

		select {
		case msg := <-responseCh:
			ctx.Data(200, "application/json", msg.Value)
			return

		case <-time.After(time.Second * 10):
			ctx.JSON(400, gin.H{"err": "failed to get patient info from db"})
			return
		}
	}
}

func (h Handler) CreatePatient() gin.HandlerFunc {
	return func(ctx *gin.Context) {
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

		h.producer.Input() <- msg

		responseCh := make(chan *sarama.ConsumerMessage)

		h.responseChan.Store(requestId, responseCh)

		select {
		case msg := <-responseCh:
			//implemet different status code
			ctx.Data(201, "application/json", msg.Value)
			return
		case <-time.After(time.Second * 10):
			ctx.JSON(400, gin.H{"err": "failed to create patient"})
			return
		}
	}
}
