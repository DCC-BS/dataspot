#!/bin/sh

# Get credentials and configuration from environment variables
AD_USERNAME="${AD_USERNAME}"
AD_PASSWORD="${AD_PASSWORD}"
AD_DOMAIN_CONTROLLER="${AD_DOMAIN_CONTROLLER}"
AD_REALM="${AD_REALM}"

# Validate all required environment variables
if [ -z "$AD_USERNAME" ]; then
    echo "ERROR: AD_USERNAME must be set"
    exit 1
fi

if [ -z "$AD_PASSWORD" ]; then
    echo "ERROR: AD_PASSWORD must be set"
    exit 1
fi

if [ -z "$AD_DOMAIN_CONTROLLER" ]; then
    echo "ERROR: AD_DOMAIN_CONTROLLER must be set"
    exit 1
fi

if [ -z "$AD_REALM" ]; then
    echo "ERROR: AD_REALM must be set"
    exit 1
fi

# Generate krb5.conf dynamically from environment variables
cat > /etc/krb5.conf << EOF
[libdefaults]
    default_realm = ${AD_REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true

[realms]
    ${AD_REALM} = {
        kdc = ${AD_DOMAIN_CONTROLLER}
        admin_server = ${AD_DOMAIN_CONTROLLER}
    }

[domain_realm]
    .$(echo ${AD_REALM} | tr '[:upper:]' '[:lower:]') = ${AD_REALM}
    $(echo ${AD_REALM} | tr '[:upper:]' '[:lower:]') = ${AD_REALM}
EOF

echo "Generated krb5.conf for realm ${AD_REALM} with KDC ${AD_DOMAIN_CONTROLLER}"

# Obtain Kerberos ticket using kinit
echo "Obtaining Kerberos ticket for ${AD_USERNAME}@${AD_REALM}..."
echo "${AD_PASSWORD}" | kinit "${AD_USERNAME}@${AD_REALM}" 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to obtain Kerberos ticket"
    exit 1
fi

echo "Kerberos ticket obtained successfully"

# Change to workdir if needed
cd /opt/workdir || true

# Run your Java application (pass through all arguments)
exec "$@"