FROM rabbitmq:3.13.7-management

USER root

# Install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create plugin directory
RUN mkdir -p /plugins

# Download delayed message plugin
RUN curl -L -o /plugins/rabbitmq_delayed_message_exchange.ez \
    https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v3.13.0/rabbitmq_delayed_message_exchange-3.13.0.ez

# Enable the plugin (offline so it works during build)
RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange

USER rabbitmq
