package com.jesusfc.kafka.handler;

import com.jesusfc.kafka.message.OrderCreated;
import com.jesusfc.kafka.service.DispatchService;
import com.jesusfc.kafka.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on jun - 2025
 */
class OrderCreatedHandlerTest {

    private OrderCreatedHandler handler;
    private DispatchService dispatchServiceMock;

    private final static String TEST_KEY = "test-key";
    private final static Integer TEST_PARTITION = 0;

    @BeforeEach
    void setUp() {
        dispatchServiceMock = mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchServiceMock);
    }

    @Test
    void listen() throws Exception {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID().toString());
        handler.listen(TEST_PARTITION, TEST_KEY, testEvent);
        verify(dispatchServiceMock, times(1)).process(TEST_PARTITION, TEST_KEY, testEvent);
    }
}