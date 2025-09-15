package com.jesusfc.kafka.service;

import com.jesusfc.kafka.message.OrderCreated;
import com.jesusfc.kafka.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on jun - 2025
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class DispatchService {


    private static final String ORDER_DISPATCHED_TOPIC = "my.order.dispatched.topic";
    private final KafkaTemplate<String, Object> kafkaProducer;

    private static final UUID APPLICATION_ID = randomUUID();

    /**
     * Desde la consola de Kafka "Producer" enviamos un mensaje JSON al topic "my.order.created.topic",
     * el listener OrderCreatedHandler lo recibe y llama al DispatchService para procesar el evento.
     * Una vez procesado, este servicio envía un nuevo mensaje al topic "my.order.dispatched.topic", el cual
     * será consumido por otro servicio o aplicación que esté escuchando ese topic, en nuestro caso, será
     * la consola de Kafka "Consumer" la que reciba el mensaje.
     *
     * Entonces, tenemos un flujo completo de mensajes en Kafka:
     * 1. Enviamos un mensaje al topic "my.order.created.topic" (Usamos la consola de Kafka Producer).
     * 2. El listener OrderCreatedHandler recibe el mensaje y llama a DispatchService.
     * 3. DispatchService procesa el mensaje y envía un nuevo mensaje al topic "my.order.dispatched.topic".
     * 4. La consola de Kafka "Consumer" recibe el mensaje del topic "my.order.dispatched.topic" y lo mostrará.
     *
     * Si tenemos dos instancias de la aplicación ejecutándose (port: 8080 y 8087),
     * ambas escuchan el mismo topic "my.order.created.topic", pero solo una de ellas procesará cada mensaje.
     * Kafka reasignará las particiones entre las instancias de la aplicación que se hayan creado. Está se unirán a un grupo
     * de consumidores (definido por groupId, "my.super.group") y Kafka distribuirá los mensajes entre las instancias.
     *
     * Si estás dos instancias estuvieran escuchando el topic "my.order.dispatched.topic", y ambas estuvieran en DIFERENTE
     * grupo de consumidores, por ejemplo, "my.super.group" y "my.super.group.2", ambas recibirían todos los mensajes y los
     * procesarían independientemente.
     *
     */
    public void process(Integer partition, String key, OrderCreated orderCreated) throws ExecutionException, InterruptedException {

        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .processedBy(APPLICATION_ID)
                .item(orderCreated.getItem() + " - dispatched")
                .notes("Dispatched: " + orderCreated.getItem())
                .build();

        log.info("Processing orderDispatched (send to another topic): {}", orderDispatched);

        // Enviamos el mensaje al topic "my.order.dispatched.topic"
        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, partition, key, orderDispatched).get();

        log.info("Send Message: orderId: {} - processedById: {}, partition: {}, key: {}", orderDispatched.getOrderId(), APPLICATION_ID, partition, key);

    }
}
