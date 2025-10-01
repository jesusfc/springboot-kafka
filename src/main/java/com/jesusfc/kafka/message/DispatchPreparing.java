package com.jesusfc.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on oct - 2025
 *
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DispatchPreparing {
    UUID orderId;
}
