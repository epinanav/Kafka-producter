package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	broker := "b-1.mskdearneuronkafka.55zwdr.c3.kafka.eu-west-1.amazonaws.com"

	// Configurar el cliente de Kafka
	conf := &kafka.ConfigMap{
		"bootstrap.servers": broker,
	}

	// Intentar crear un productor Kafka
	p, err := kafka.NewProducer(conf)
	if err != nil {
		fmt.Printf("Error al crear el productor Kafka: %v\n", err)
		return
	}
	

	fmt.Println("Conexión con Kafka establecida exitosamente.")

	// Definir el mensaje a enviar
	topic := "WDN"
	id := "1"
	message := "Hola, Kafka!"

	// Publicar el mensaje en Kafka
	err = publishMessage(p, topic, id, message)
	if err != nil {
		fmt.Printf("Error al publicar el mensaje en Kafka: %v\n", err)
		return
	}

	fmt.Println("Mensaje publicado exitosamente en el topic", topic, "con ID", id)
}

func publishMessage(p *kafka.Producer, topic, id, message string) error {
	// Enviar el mensaje al broker de Kafka
	deliveryChan := make(chan kafka.Event)
	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
		Key:            []byte(id),
	}, deliveryChan)

	if err != nil {
		return err
	}

	// Esperar a que el mensaje sea entregado
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}

	return nil
}