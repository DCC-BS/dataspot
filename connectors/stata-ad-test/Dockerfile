FROM eclipse-temurin:17-jre-alpine
WORKDIR /opt/workdir

# Install Kerberos client libraries
RUN apk add --no-cache krb5 krb5-libs

# Copy krb5.conf
COPY krb5.conf /etc/krb5.conf

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Label to automatically link the package to the dataspot repository in GHCR
# This avoids the need to manually link the package after pushing
LABEL org.opencontainers.image.source=https://github.com/dcc-bs/dataspot
