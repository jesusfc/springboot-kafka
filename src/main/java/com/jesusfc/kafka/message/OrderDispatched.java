package com.jesusfc.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Author Jesús Fdez. Caraballo
 * jesus.fdez.caraballo@gmail.com
 * Created on jun - 2025
 */
/* Example of OrderDispatched message:
{
        "orderId": "b8e3d0c3-9241-4eeb-8a5b-7c412c3a8a24",
        "item": "Red"
        }
*/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDispatched {

    UUID orderId;
    String item;
    String notes;
    UUID processedBy;
}

