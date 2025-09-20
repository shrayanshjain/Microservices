package com.concepts;

import com.concepts.extensions.LifecycleExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LifecycleExtension.class)
public class MyServiceTest {

    @Test
    void testMethod() {

        System.out.println("inside test method");
    }
}

