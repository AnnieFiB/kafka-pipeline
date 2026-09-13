# spark/Dockerfile
FROM bitnami/spark:3.5.1

USER root
# Install Python 3 + pip (Debian base)
RUN install_packages python3 python3-venv python3-pip && \
    ln -sf /usr/bin/python3 /usr/local/bin/python3 && \
    ln -sf /usr/bin/pip3 /usr/local/bin/pip3
USER 1001
