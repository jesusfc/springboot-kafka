package com.jesusfc.kafka.service;


import com.jesusfc.kafka.client.StockServiceClient;
import com.jesusfc.kafka.message.OrderCreated;
import com.jesusfc.kafka.message.OrderDispatched;
import com.jesusfc.kafka.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on jun - 2025
 */
class DispatchServiceTest {

    private DispatchService dispatchService;
    private KafkaTemplate kafkaTemplateMock;
    private StockServiceClient stockServiceClientMock;

    private final static String TOPIC = "my.order.dispatched.topic";
    private final static String TEST_KEY = "test-key";
    private final static Integer TEST_PARTITION = 0;

    @BeforeEach
    void setUp() {
        kafkaTemplateMock = mock(KafkaTemplate.class);
        stockServiceClientMock = mock(StockServiceClient.class);
        dispatchService = new DispatchService(kafkaTemplateMock, stockServiceClientMock);
    }

    @Test
    void process() throws Exception {

        CompletableFuture<Void> futureMock = mock(CompletableFuture.class);
        when(kafkaTemplateMock.send(anyString(), anyInt(), anyString(), any(OrderDispatched.class))).thenReturn(futureMock);

        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID().toString());
        dispatchService.process(TEST_PARTITION, TEST_KEY, testEvent);

        verify(kafkaTemplateMock, times(1)).send(anyString(), anyInt(), anyString(), any(OrderDispatched.class));
    }

    @Test
    void process_ProducerThrowsException() {

        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID().toString());
        doThrow(new RuntimeException("Kafka producer error")).when(kafkaTemplateMock).send(eq(TOPIC), anyInt(), anyString(), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(TEST_PARTITION, TEST_KEY, testEvent));

        verify(kafkaTemplateMock, times(1)).send(eq(TOPIC), anyInt(), anyString(), any(OrderDispatched.class));
        assertThat(exception.getMessage()).contains("Kafka producer error");


    }
}