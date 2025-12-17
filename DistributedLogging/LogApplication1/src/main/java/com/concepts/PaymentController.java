package com.concepts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentController {

    Logger log = LoggerFactory.getLogger(PaymentController.class);

    @GetMapping("/payments")
    public String getPayments() {

        MDC.put("payment_id", "123");
        MDC.put("password", "myPassword");
        log.info("payment is successful");

        return "successfully fetched all payments";
    }

    public String logsWithPlaceholderSamples() {

        log.info("info log");

        /** never do this
         * why because it will create the string even if the log is not enabled
         * Say our Logger Level is set to WARN,
         * then also it will create the string and then throw it away
         */
        log.info("User" + "Shrayansh" + "created with id" +  123);


        //recommended way, because it will not create the string if the log is not enabled
        log.info("User {} created with id {}", "Shrayansh", 123);


        try {
            throw new Exception("Payment failed");
        } catch (Exception e) {
            /** note: exception is not the placeholder
             *  and its always the last parameter
             *  then only Stack trace will be printed
             */
            log.error("Payment failed for User {}", "Shrayansh", e);
        }


        return "successfully fetched all payments";
    }
}





