package com.jesusfc.kafka.handler;

import com.jesusfc.kafka.message.OrderCreated;
import com.jesusfc.kafka.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on jun - 2025
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    /*
     * This method listens to the "my.order.created.topic" Kafka topic for messages of type OrderCreated.
     * It processes the incoming OrderCreated message by calling the DispatchService.
     *
     * CREAMOS UN CONSUMIDOR DE KAFKA QUE ESCUCHA EL TOPIC "my.order.created.topic"
     * La anotación @KafkaListener indica que este método es un consumidor de Kafka.
     * El parámetro "id" es un identificador único para este consumidor.
     * El parámetro "topics" especifica el tema de Kafka al que se suscribe.
     * El parámetro "groupId" define el grupo de consumidores al que pertenece este consumidor. Si
     * varios consumidores tienen el mismo groupId, se distribuirán los mensajes entre ellos. Se distribuye la
     * carga de procesamiento entre las diferentes instancias. Pero si tienen diferentes groupId,
     * cada consumidor recibirá una copia de cada mensaje. Esto permite escalar el procesamiento de mensajes
     * y asegurar que cada mensaje sea procesado al menos una vez.
     * El parámetro "containerFactory" especifica la fábrica de contenedores que se utilizará para crear el contenedor del consumidor.
     */
    @KafkaListener(
            id = "orderConsumerClient",
            topics = "${spring.kafka.topics.consumer}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                       @Header(KafkaHeaders.RECEIVED_KEY) String key,
                       @Payload OrderCreated payload) {

        try {

            log.info("Processing order created event, partition: {}, key: {}, payload: {}", partition, key, payload);
            dispatchService.process(partition, key, payload);

        } catch (Exception e) {
            log.error("Error processing order created event: {}", payload, e);
        }
    }
}
