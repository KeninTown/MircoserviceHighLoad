package handlers

import (
	"HighLoadServer/internal/entities"
	"encoding/json"
	"fmt"
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
		fmt.Println("chanId = ", requestId)

		select {
		case msg := <-responseCh:
			//implemet different status code
			h.responseChan.Delete(requestId)

			var patient entities.Patient
			if err := json.Unmarshal(msg.Value, &patient); err != nil || patient.Id == 0 {

				var responseErr Error
				if err := json.Unmarshal(msg.Value, &responseErr); err != nil || responseErr.Err == "" {
					ctx.JSON(500, "unexpected error")
					return
				}

				ctx.JSON(400, responseErr)
				return
			}

			ctx.JSON(200, patient)
			return
		case <-time.After(time.Second * 10):
			h.responseChan.Delete(requestId)
			ctx.JSON(400, NewResponseErr("failed to get response"))
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
		fmt.Println("Send chanId = ", requestId)

		select {
		case msg := <-responseCh:
			h.responseChan.Delete(requestId)
			//implemet different status code
			var patient entities.Patient
			if err := json.Unmarshal(msg.Value, &patient); err != nil {
				slog.Error(err.Error())

				var responseErr Error
				if err := json.Unmarshal(msg.Value, &responseErr); err != nil || responseErr.Err == "" {
					ctx.JSON(500, "unexpected error")
					return
				}

				ctx.JSON(400, responseErr)
				return
			}

			if patient.Id == 0 {
				fmt.Print("зашли")
				var responseErr Error
				if err := json.Unmarshal(msg.Value, &responseErr); err != nil || responseErr.Err == "" {
					ctx.JSON(500, "unexpected error")
					return
				}

				ctx.JSON(400, responseErr)
				return
			}

			ctx.JSON(201, patient)
			return
		case <-time.After(time.Second * 10):
			ctx.JSON(400, NewResponseErr("failed to get response"))
			h.responseChan.Delete(requestId)
			return
		}
	}
}
