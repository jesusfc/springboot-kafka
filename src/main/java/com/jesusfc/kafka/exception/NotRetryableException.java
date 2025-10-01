package com.jesusfc.kafka.exception;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on oct - 2025
 *
 */
/*
Se utiliza para representar errores que no deben ser reintentados en la lógica de la aplicación Kafka.
Al crear una instancia de esta excepción, se le pasa otra excepción como causa,
permitiendo mantener el stacktrace original.
Su uso típico es lanzar esta excepción cuando ocurre un error que no tiene sentido reintentar,
por ejemplo, errores de validación o de configuración.
Un constructor que recibe una excepción y la pasa al constructor de la clase padre (RuntimeException).
 */
public class NotRetryableException extends RuntimeException {

    public NotRetryableException(Exception exception) {
        super(exception);
    }
}
