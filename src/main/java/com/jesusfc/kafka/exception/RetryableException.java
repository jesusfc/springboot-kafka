package com.jesusfc.kafka.exception;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on oct - 2025
 *
 */
/*
Se utiliza para indicar errores que pueden ser reintentados en la lógica de la aplicación,
por ejemplo, en operaciones con Kafka donde un fallo podría solucionarse al intentar de nuevo.
Incluye dos constructores:
- Uno que recibe un mensaje de error (String message).
- Otro que recibe una excepción (Exception exception) y la pasa como causa.
No tiene métodos adicionales ni lógica extra. Su propósito es diferenciar los errores que
sí pueden ser reintentados de otros tipos de excepciones.
 */
public class RetryableException extends RuntimeException {

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Exception exception) {
        super(exception);
    }
}