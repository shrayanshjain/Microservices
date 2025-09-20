package com.concepts.extensions;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ConditionalExecutionExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

        boolean featureEnabled = Boolean.parseBoolean(
                context.getConfigurationParameter("xyz.feature.enabled").orElse("false"));

        return featureEnabled
                ? ConditionEvaluationResult.enabled("feature enabled, test can run")
                : ConditionEvaluationResult.disabled("feature disabled, test can not run");
    }
}



