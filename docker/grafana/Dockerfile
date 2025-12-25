FROM grafana/grafana:latest

USER root

# Копируем provisioning конфигурации
COPY docker/grafana/provisioning /etc/grafana/provisioning

USER grafana
