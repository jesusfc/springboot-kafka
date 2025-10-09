package com.jesusfc.kafka.service;


import com.jesusfc.kafka.client.StockServiceClient;
import com.jesusfc.kafka.message.DispatchCompleted;
import com.jesusfc.kafka.message.DispatchPreparing;
import com.jesusfc.kafka.message.OrderCreated;
import com.jesusfc.kafka.message.OrderDispatched;
import com.jesusfc.kafka.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
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

        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatchService.process(TEST_PARTITION, TEST_KEY, testEvent);

        verify(kafkaTemplateMock, times(1)).send(anyString(), anyInt(), anyString(), any(OrderDispatched.class));
    }

    @Test
    void process_ProducerThrowsException() {

        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Kafka producer error")).when(kafkaTemplateMock).send(eq(TOPIC), anyInt(), anyString(), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(TEST_PARTITION, TEST_KEY, testEvent));

        verify(kafkaTemplateMock, times(1)).send(eq(TOPIC), anyInt(), anyString(), any(OrderDispatched.class));
        assertThat(exception.getMessage()).contains("Kafka producer error");


    }

    @Test
    void process_Success() throws Exception {
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaTemplateMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchCompleted.class))).thenReturn(mock(CompletableFuture.class));
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");

        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatchService.process(TEST_PARTITION, key, testEvent);

        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
    }

    @Test
    public void testProcess_StockUnavailable() throws Exception {
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("false");

        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatchService.process(TEST_PARTITION, key, testEvent);
        verifyNoInteractions(kafkaTemplateMock);
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
    }

    @Test
    public void testProcess_DispatchTrackingProducerThrowsException() {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaTemplateMock).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(TEST_PARTITION, key, testEvent));

        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verifyNoMoreInteractions(kafkaTemplateMock);
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
        assertThat(exception.getMessage()).isEqualTo("dispatch tracking producer failure");
        assertThat(exception.getMessage()).isEqualTo("order dispatched producer failure");
        assertThat(exception.getMessage()).isEqualTo("dispatch tracking producer failure");
        assertThat(exception.getMessage()).isEqualTo("stock service client failure");
    }

    @Test
    public void testProcess_OrderDispatchedProducerThrowsException() {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        doThrow(new RuntimeException("order dispatched producer failure")).when(kafkaTemplateMock).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(TEST_PARTITION, key, testEvent));

        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verifyNoMoreInteractions(kafkaTemplateMock);
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
        assertThat(exception.getMessage()).isEqualTo("order dispatched producer failure");
    }

    @Test
    public void testProcess_SecondDispatchTrackingProducerThrowsException() {
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaTemplateMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");

        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaTemplateMock).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(TEST_PARTITION, key, testEvent));

        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
        assertThat(exception.getMessage()).isEqualTo("dispatch tracking producer failure");
    }

    @Test
    public void testProcess_StockServiceClient_ThrowsException() {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());

        doThrow(new RuntimeException("stock service client failure")).when(stockServiceClientMock).checkAvailability(testEvent.getItem());

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(TEST_PARTITION, key, testEvent));
        assertThat(exception.getMessage()).isEqualTo("stock service client failure");

        verifyNoInteractions(kafkaTemplateMock);
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
    }
}