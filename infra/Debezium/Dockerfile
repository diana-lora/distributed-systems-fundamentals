FROM quay.io/debezium/connect:2.7.3.Final

# Switch to root to ensure file write permissions succeed
USER root

# Run download in a single flat line with no hidden spacing breaks
RUN curl -fSLo /kafka/libs/debezium-scripting-2.7.3.Final.jar https://maven.org

# Switch execution context back to the default kafka user
USER kafka
