Below is a production-oriented runbook designed for **Confluent Platform 7.9 using KRaft on an on-premises OpenShift cluster**. It assumes Confluent for Kubernetes manages the Kafka resources. Where deployments are not CFK-managed, the same certificate-generation and validation procedures apply, but rollout commands must target the applicable StatefulSets or Deployments.

# Production Kafka TLS Certificate Renewal and Deployment Runbook

## 1. Purpose

This runbook defines the process for:

1. Inventorying the existing Kafka TLS configuration.
2. Generating a new private key and certificate signing request.
3. Submitting the CSR to an enterprise certificate authority.
4. Validating the signed certificate and certificate chain.
5. Updating OpenShift secrets used by Confluent Kafka.
6. Rolling or dynamically applying the certificate.
7. Updating Kafka client truststores where required.
8. Testing broker, client, producer, consumer, and administrative connectivity.
9. Rolling back if the deployment is unsuccessful.

The procedure is intended for a production Confluent Kafka cluster running in an on-premises OpenShift environment.

---

# 2. Important TLS Concepts

Kafka TLS deployments can use either:

* **One-way TLS:** Clients validate the Kafka broker certificate.
* **Mutual TLS:** Clients validate the broker, and the broker validates a client certificate.

A routine Kafka server-certificate renewal normally requires client updates only when:

* The new server certificate is signed by a different root or intermediate CA.
* The existing client truststore does not contain the new CA chain.
* The broker DNS names or Subject Alternative Names change.
* Certificate pinning is used.
* Applications bundle certificate files directly instead of using a shared truststore.

Kafka hostname verification evaluates the hostname used by the client against the certificate's Subject Alternative Name entries. Every bootstrap and advertised broker hostname used by clients must therefore appear in the certificate SAN list.

Confluent for Kubernetes supports user-provided TLS material through Kubernetes secrets. Updating the referenced secret is the standard certificate-rotation mechanism. Depending on the CFK version and configuration, the update can trigger a controlled rolling restart or dynamic certificate rotation. ([Confluent Documentation][1])

---

# 3. Roles and Responsibilities

| Role                     | Responsibility                                                  |
| ------------------------ | --------------------------------------------------------------- |
| Kafka platform team      | Inventory, CSR creation, deployment, broker testing, rollback   |
| OpenShift administrators | Secret access, resource validation, rollout monitoring          |
| Enterprise PKI team      | CSR approval, certificate signing, CA-chain delivery            |
| Application teams        | Client truststore updates and application testing               |
| Security team            | Certificate-policy review and change approval                   |
| Change manager           | Production window, communications, approvals, go/no-go decision |

The private key should remain under the control of the Kafka platform team and should not be sent to the certificate authority.

---

# 4. Required Information

Collect the following before generating the certificate.

## 4.1 OpenShift and Kafka Information

```bash
export KAFKA_NAMESPACE="confluent-prod"
export KAFKA_CR_NAME="kafka"
export TLS_SECRET_NAME="kafka-tls"
export WORK_DIR="$HOME/kafka-cert-rotation-$(date +%Y%m%d)"
```

Create the working directory:

```bash
mkdir -p "${WORK_DIR}"
chmod 700 "${WORK_DIR}"
cd "${WORK_DIR}"
```

## 4.2 Certificate Information

```bash
export CERT_COMMON_NAME="kafka-bootstrap.prod.example.com"
export CERT_ORGANIZATION="Example Corporation"
export CERT_ORG_UNIT="Enterprise Platforms"
export CERT_LOCALITY="Annapolis"
export CERT_STATE="Maryland"
export CERT_COUNTRY="US"
export CERT_DAYS="397"
```

The certificate authority may enforce a shorter validity period.

## 4.3 Required DNS Names

The final list must include every hostname presented to clients, including:

* External bootstrap hostname.
* Internal bootstrap service.
* Each externally advertised broker hostname.
* Internal broker service names where internal clients validate hostnames.
* Kafka REST or MDS names only when those services use the same certificate.

Example:

```text
kafka-bootstrap.prod.example.com
kafka-0.prod.example.com
kafka-1.prod.example.com
kafka-2.prod.example.com
kafka.confluent-prod.svc
kafka.confluent-prod.svc.cluster.local
```

Do not assume that a wildcard certificate covers OpenShift service names unless the wildcard actually matches those names.

---

# 5. Pre-Change Discovery

## 5.1 Authenticate to OpenShift

```bash
oc login https://api.openshift.example.com:6443
oc project "${KAFKA_NAMESPACE}"
```

Confirm context:

```bash
oc whoami
oc project
oc cluster-info
```

## 5.2 Locate the Kafka Resource

For CFK-managed Kafka:

```bash
oc get kafka -n "${KAFKA_NAMESPACE}"
oc get kafka "${KAFKA_CR_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o yaml > "${WORK_DIR}/kafka-cr-before.yaml"
```

Find TLS-related configuration:

```bash
oc get kafka "${KAFKA_CR_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o yaml |
grep -nE 'tls|secretRef|certificate|listener|externalAccess'
```

Review all secrets referenced by the Kafka custom resource:

```bash
oc get kafka "${KAFKA_CR_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o jsonpath='{.spec}' |
python3 -m json.tool |
grep -i -B3 -A5 secret
```

## 5.3 Inventory Kafka Pods and Services

```bash
oc get pods -n "${KAFKA_NAMESPACE}" -o wide
oc get svc -n "${KAFKA_NAMESPACE}"
oc get routes -n "${KAFKA_NAMESPACE}"
```

Save the results:

```bash
oc get pods -n "${KAFKA_NAMESPACE}" -o wide \
  > "${WORK_DIR}/pods-before.txt"

oc get svc -n "${KAFKA_NAMESPACE}" -o yaml \
  > "${WORK_DIR}/services-before.yaml"

oc get routes -n "${KAFKA_NAMESPACE}" -o yaml \
  > "${WORK_DIR}/routes-before.yaml"
```

## 5.4 Back Up the Existing TLS Secret

```bash
oc get secret "${TLS_SECRET_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o yaml > "${WORK_DIR}/${TLS_SECRET_NAME}-backup.yaml"
```

Secure the backup:

```bash
chmod 600 "${WORK_DIR}/${TLS_SECRET_NAME}-backup.yaml"
```

Do not commit this file to Git because it contains the private key.

## 5.5 Identify Secret Key Names

```bash
oc get secret "${TLS_SECRET_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o jsonpath='{range $key,$value := .data}{$key}{"\n"}{end}'
```

Common CFK PEM secret keys include:

```text
fullchain.pem
cacerts.pem
privkey.pem
```

The exact names must match the CFK configuration and certificate format currently in use. Do not substitute `tls.crt` and `tls.key` without confirming the custom resource expects a Kubernetes TLS-type secret.

## 5.6 Export and Inspect the Existing Certificate

For a CFK PEM secret:

```bash
oc get secret "${TLS_SECRET_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o jsonpath='{.data.fullchain\.pem}' |
base64 -d > "${WORK_DIR}/current-fullchain.pem"
```

Inspect it:

```bash
openssl x509 \
  -in "${WORK_DIR}/current-fullchain.pem" \
  -noout \
  -subject \
  -issuer \
  -serial \
  -dates \
  -fingerprint \
  -sha256 \
  -ext subjectAltName
```

Record the existing certificate fingerprint:

```bash
openssl x509 \
  -in "${WORK_DIR}/current-fullchain.pem" \
  -noout \
  -fingerprint \
  -sha256 |
tee "${WORK_DIR}/old-certificate-fingerprint.txt"
```

## 5.7 Capture the Certificate Currently Presented by Kafka

```bash
export KAFKA_BOOTSTRAP_HOST="kafka-bootstrap.prod.example.com"
export KAFKA_BOOTSTRAP_PORT="9093"
```

```bash
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  -showcerts </dev/null 2>&1 |
tee "${WORK_DIR}/openssl-before.txt"
```

Extract the leaf certificate:

```bash
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  </dev/null 2>/dev/null |
openssl x509 -outform PEM \
  > "${WORK_DIR}/presented-certificate-before.pem"
```

---

# 6. Generate the Private Key and CSR

## 6.1 Create the OpenSSL Configuration

Create `kafka-server-csr.cnf`:

```bash
cat > "${WORK_DIR}/kafka-server-csr.cnf" <<EOF
[ req ]
default_bits       = 3072
prompt             = no
default_md         = sha256
distinguished_name = subject
req_extensions     = request_extensions

[ subject ]
C  = ${CERT_COUNTRY}
ST = ${CERT_STATE}
L  = ${CERT_LOCALITY}
O  = ${CERT_ORGANIZATION}
OU = ${CERT_ORG_UNIT}
CN = ${CERT_COMMON_NAME}

[ request_extensions ]
subjectAltName = @subject_alt_names
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth

[ subject_alt_names ]
DNS.1 = kafka-bootstrap.prod.example.com
DNS.2 = kafka-0.prod.example.com
DNS.3 = kafka-1.prod.example.com
DNS.4 = kafka-2.prod.example.com
DNS.5 = kafka.confluent-prod.svc
DNS.6 = kafka.confluent-prod.svc.cluster.local
EOF
```

Adjust the SAN list to match the actual production listeners and advertised broker names.

## 6.2 Generate the Private Key

```bash
umask 077

openssl genpkey \
  -algorithm RSA \
  -pkeyopt rsa_keygen_bits:3072 \
  -out "${WORK_DIR}/server-key.pem"
```

Validate the private key:

```bash
openssl pkey \
  -in "${WORK_DIR}/server-key.pem" \
  -check \
  -noout
```

## 6.3 Generate the CSR

```bash
openssl req \
  -new \
  -key "${WORK_DIR}/server-key.pem" \
  -out "${WORK_DIR}/server.csr" \
  -config "${WORK_DIR}/kafka-server-csr.cnf"
```

Inspect the CSR:

```bash
openssl req \
  -in "${WORK_DIR}/server.csr" \
  -noout \
  -text
```

Display the SAN extension specifically:

```bash
openssl req \
  -in "${WORK_DIR}/server.csr" \
  -noout \
  -text |
sed -n '/Subject Alternative Name/,+2p'
```

Generate a CSR fingerprint:

```bash
openssl req \
  -in "${WORK_DIR}/server.csr" \
  -noout \
  -fingerprint \
  -sha256
```

---

# 7. CSR Submission Template

Submit only the following to the enterprise PKI team:

```text
Certificate type: TLS server certificate
Platform: Confluent Kafka on OpenShift
Environment: Production
Key algorithm: RSA 3072
Signature algorithm requested: SHA-256
Extended key usage: TLS Web Server Authentication/serverAuth
Common Name: kafka-bootstrap.prod.example.com

Required Subject Alternative Names:
- kafka-bootstrap.prod.example.com
- kafka-0.prod.example.com
- kafka-1.prod.example.com
- kafka-2.prod.example.com
- kafka.confluent-prod.svc
- kafka.confluent-prod.svc.cluster.local

Requested deliverables:
- Signed leaf/server certificate in PEM format
- Issuing intermediate CA certificate or certificates
- Root CA certificate
- Complete CA chain in PEM format

The private key was generated by the platform team and is not included.
```

Attach:

```text
server.csr
```

Do not attach:

```text
server-key.pem
```

---

# 8. Validate the Signed Certificate

Assume the CA returns:

```text
server.pem
intermediate-ca.pem
root-ca.pem
```

## 8.1 Confirm Certificate Details

```bash
openssl x509 \
  -in "${WORK_DIR}/server.pem" \
  -noout \
  -subject \
  -issuer \
  -serial \
  -dates \
  -fingerprint \
  -sha256 \
  -ext subjectAltName \
  -ext extendedKeyUsage
```

Confirm that:

* The certificate is not expired.
* The certificate begins before the planned deployment date.
* All required DNS SANs are present.
* Extended Key Usage includes server authentication.
* The certificate issuer is the expected enterprise CA.

## 8.2 Verify the Certificate Matches the Private Key

For RSA keys:

```bash
openssl pkey \
  -in "${WORK_DIR}/server-key.pem" \
  -pubout \
  -outform DER |
openssl sha256
```

```bash
openssl x509 \
  -in "${WORK_DIR}/server.pem" \
  -pubkey \
  -noout |
openssl pkey \
  -pubin \
  -outform DER |
openssl sha256
```

The two hashes must be identical.

A reusable validation script:

```bash
cat > "${WORK_DIR}/verify-key-match.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

PRIVATE_KEY="${1:?Usage: $0 private-key.pem certificate.pem}"
CERTIFICATE="${2:?Usage: $0 private-key.pem certificate.pem}"

KEY_HASH="$(
  openssl pkey -in "${PRIVATE_KEY}" -pubout -outform DER 2>/dev/null |
  openssl sha256
)"

CERT_HASH="$(
  openssl x509 -in "${CERTIFICATE}" -pubkey -noout |
  openssl pkey -pubin -outform DER 2>/dev/null |
  openssl sha256
)"

echo "Private key public-key hash: ${KEY_HASH}"
echo "Certificate public-key hash: ${CERT_HASH}"

if [[ "${KEY_HASH}" != "${CERT_HASH}" ]]; then
  echo "ERROR: Certificate does not match private key." >&2
  exit 1
fi

echo "SUCCESS: Certificate matches private key."
EOF

chmod 700 "${WORK_DIR}/verify-key-match.sh"
```

Run it:

```bash
"${WORK_DIR}/verify-key-match.sh" \
  "${WORK_DIR}/server-key.pem" \
  "${WORK_DIR}/server.pem"
```

## 8.3 Build the CA Chain

Create a CA bundle containing intermediate certificates followed by the root:

```bash
cat \
  "${WORK_DIR}/intermediate-ca.pem" \
  "${WORK_DIR}/root-ca.pem" \
  > "${WORK_DIR}/cacerts.pem"
```

Create the full chain containing the leaf certificate followed by intermediate certificates:

```bash
cat \
  "${WORK_DIR}/server.pem" \
  "${WORK_DIR}/intermediate-ca.pem" \
  > "${WORK_DIR}/fullchain.pem"
```

Do not normally append the root CA to the server-presented full chain unless the enterprise PKI standard explicitly requires it.

## 8.4 Verify the Chain

```bash
openssl verify \
  -CAfile "${WORK_DIR}/root-ca.pem" \
  -untrusted "${WORK_DIR}/intermediate-ca.pem" \
  "${WORK_DIR}/server.pem"
```

Expected result:

```text
server.pem: OK
```

Validate the assembled fullchain:

```bash
openssl crl2pkcs7 \
  -nocrl \
  -certfile "${WORK_DIR}/fullchain.pem" |
openssl pkcs7 \
  -print_certs \
  -noout
```

## 8.5 Check File Permissions

```bash
chmod 600 \
  "${WORK_DIR}/server-key.pem" \
  "${WORK_DIR}/server.pem" \
  "${WORK_DIR}/fullchain.pem" \
  "${WORK_DIR}/cacerts.pem"
```

---

# 9. Determine Whether Client Truststores Must Change

Compare the old and new issuers:

```bash
echo "Old certificate:"
openssl x509 \
  -in "${WORK_DIR}/current-fullchain.pem" \
  -noout \
  -issuer

echo "New certificate:"
openssl x509 \
  -in "${WORK_DIR}/server.pem" \
  -noout \
  -issuer
```

Clients generally do not need a truststore update when:

* The new certificate chains to a CA already trusted by the clients.
* The hostname remains unchanged.
* No certificate pinning is used.

Clients do require an update when:

* The root CA changes.
* A new intermediate CA is introduced and the server does not provide it correctly.
* The client truststore contains only the old leaf certificate.
* The application explicitly pins a certificate fingerprint.

Kafka clients support truststores in JKS or PKCS12 format and can also use PEM trust material, depending on the client and configuration. Client keystores are required for mutual TLS authentication. ([Apache Kafka][2])

---

# 10. Create or Update Client Truststores

## 10.1 Java PKCS12 Truststore

```bash
export CLIENT_TRUSTSTORE="${WORK_DIR}/kafka-client-truststore.p12"
export CLIENT_TRUSTSTORE_PASSWORD='REPLACE_WITH_SECURE_PASSWORD'
```

Create the truststore:

```bash
keytool -importcert \
  -noprompt \
  -alias enterprise-root-ca \
  -file "${WORK_DIR}/root-ca.pem" \
  -keystore "${CLIENT_TRUSTSTORE}" \
  -storetype PKCS12 \
  -storepass "${CLIENT_TRUSTSTORE_PASSWORD}"
```

Import the intermediate CA:

```bash
keytool -importcert \
  -noprompt \
  -alias enterprise-kafka-intermediate-ca \
  -file "${WORK_DIR}/intermediate-ca.pem" \
  -keystore "${CLIENT_TRUSTSTORE}" \
  -storetype PKCS12 \
  -storepass "${CLIENT_TRUSTSTORE_PASSWORD}"
```

Inspect the truststore:

```bash
keytool -list \
  -v \
  -keystore "${CLIENT_TRUSTSTORE}" \
  -storetype PKCS12 \
  -storepass "${CLIENT_TRUSTSTORE_PASSWORD}"
```

## 10.2 Update an Existing Truststore

Back it up first:

```bash
cp kafka-client-truststore.p12 \
   kafka-client-truststore.p12.$(date +%Y%m%d%H%M%S).bak
```

Check whether the alias exists:

```bash
keytool -list \
  -keystore kafka-client-truststore.p12 \
  -storetype PKCS12 \
  -storepass "${CLIENT_TRUSTSTORE_PASSWORD}" \
  -alias enterprise-kafka-intermediate-ca
```

Delete a stale alias when necessary:

```bash
keytool -delete \
  -alias enterprise-kafka-intermediate-ca \
  -keystore kafka-client-truststore.p12 \
  -storetype PKCS12 \
  -storepass "${CLIENT_TRUSTSTORE_PASSWORD}"
```

Import the new certificate:

```bash
keytool -importcert \
  -noprompt \
  -alias enterprise-kafka-intermediate-ca \
  -file "${WORK_DIR}/intermediate-ca.pem" \
  -keystore kafka-client-truststore.p12 \
  -storetype PKCS12 \
  -storepass "${CLIENT_TRUSTSTORE_PASSWORD}"
```

For a CA transition, retain both the old and new CA certificates during the overlap period.

---

# 11. Pre-Production Testing

Before production deployment, perform the following in a lower environment using certificates issued from the same CA hierarchy:

1. Update the non-production Kafka TLS secret.
2. Verify all brokers become ready.
3. Confirm the presented certificate.
4. Test Java, Python, command-line, connector, and monitoring clients.
5. Verify hostname validation remains enabled.
6. Confirm mTLS client identities are unaffected.
7. Verify Control Center, Connect, Schema Registry, REST Proxy, MDS, and monitoring integrations where applicable.

Do not disable hostname validation to make a failed test pass. That masks SAN and advertised-listener problems.

---

# 12. Production Change Preparation

## 12.1 Recommended Change Window Conditions

The production change should begin only when:

* The signed certificate has passed all offline checks.
* The old certificate has not expired.
* The existing TLS secret has been backed up.
* The Kafka CR has been backed up.
* The new CA has been distributed to clients when required.
* Application owners are available for validation.
* Kafka replication and cluster health are normal.
* No partition reassignments or major maintenance activities are running.

## 12.2 Establish the Baseline

Run a Kafka metadata check before the change:

```bash
kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --list > "${WORK_DIR}/topics-before.txt"
```

Check under-replicated partitions:

```bash
kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --describe |
grep -E 'UnderReplicatedPartitions|Leader: -1' || true
```

Where Confluent CLI is available, capture cluster status and broker information as well.

---

# 13. Update the OpenShift TLS Secret

## 13.1 CFK PEM Secret

Confluent documents the following logical PEM mapping for user-provided certificates:

```text
fullchain.pem = leaf certificate plus intermediate certificates
cacerts.pem   = trusted CA certificates
privkey.pem   = private key
```

Update the secret declaratively:

```bash
oc create secret generic "${TLS_SECRET_NAME}" \
  --namespace "${KAFKA_NAMESPACE}" \
  --from-file=fullchain.pem="${WORK_DIR}/fullchain.pem" \
  --from-file=cacerts.pem="${WORK_DIR}/cacerts.pem" \
  --from-file=privkey.pem="${WORK_DIR}/server-key.pem" \
  --dry-run=client \
  -o yaml |
oc apply -f -
```

This pattern preserves the secret name referenced by the Kafka custom resource. CFK monitors the secret and processes the certificate change. ([Confluent Documentation][3])

Verify secret metadata:

```bash
oc get secret "${TLS_SECRET_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -o jsonpath='{.metadata.resourceVersion}{"\n"}'
```

## 13.2 Standard Kubernetes TLS Secret

Use this only when the Kafka configuration explicitly expects a `kubernetes.io/tls` secret:

```bash
oc create secret tls "${TLS_SECRET_NAME}" \
  --namespace "${KAFKA_NAMESPACE}" \
  --cert="${WORK_DIR}/fullchain.pem" \
  --key="${WORK_DIR}/server-key.pem" \
  --dry-run=client \
  -o yaml |
oc apply -f -
```

A standard TLS secret uses:

```text
tls.crt
tls.key
```

OpenShift supports creating TLS secrets through `oc create secret tls`, but the target application must expect that format. ([Red Hat Documentation][4])

---

# 14. Monitor the CFK Reconciliation

## 14.1 Watch the Kafka Resource

```bash
oc get kafka "${KAFKA_CR_NAME}" \
  -n "${KAFKA_NAMESPACE}" \
  -w
```

## 14.2 Watch Kafka Pods

In a second terminal:

```bash
oc get pods \
  -n "${KAFKA_NAMESPACE}" \
  -w
```

## 14.3 Monitor CFK Operator Logs

Locate the operator:

```bash
oc get pods -A |
grep -E 'confluent-operator|confluent-for-kubernetes'
```

Then monitor it:

```bash
oc logs \
  -n confluent \
  deployment/confluent-operator \
  --follow
```

Adjust the operator namespace and deployment name as required.

Look for:

* Secret-change detection.
* Certificate parsing errors.
* Private-key mismatch errors.
* Invalid certificate chain errors.
* Broker rolling-restart activity.
* Readiness failures.
* Listener reconfiguration errors.

## 14.4 Dynamic Rotation Versus Rolling Restart

CFK can be configured to dynamically rotate listener certificates, but the feature is not necessarily enabled by default. When dynamic rotation is not enabled or supported by the deployed CFK version, certificate changes can result in a broker rolling restart. ([Confluent Documentation][3])

Do not manually restart all brokers simultaneously.

If a manual rollout is required for a non-CFK deployment, restart one broker at a time:

```bash
oc delete pod <broker-pod-name> \
  -n "${KAFKA_NAMESPACE}"
```

Wait for that broker to become fully ready and rejoin the cluster before proceeding to the next broker.

For CFK deployments, prefer allowing CFK to control the rollout.

---

# 15. Validate the New Server Certificate

## 15.1 Retrieve the Presented Certificate

```bash
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  -showcerts </dev/null 2>&1 |
tee "${WORK_DIR}/openssl-after.txt"
```

Extract and inspect the leaf certificate:

```bash
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  </dev/null 2>/dev/null |
openssl x509 \
  -noout \
  -subject \
  -issuer \
  -serial \
  -dates \
  -fingerprint \
  -sha256 \
  -ext subjectAltName
```

Confirm that the fingerprint matches the new certificate:

```bash
echo "Expected:"
openssl x509 \
  -in "${WORK_DIR}/server.pem" \
  -noout \
  -fingerprint \
  -sha256

echo "Presented:"
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  </dev/null 2>/dev/null |
openssl x509 \
  -noout \
  -fingerprint \
  -sha256
```

## 15.2 Verify Hostname and Trust

```bash
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  -verify_hostname "${KAFKA_BOOTSTRAP_HOST}" \
  -CAfile "${WORK_DIR}/cacerts.pem" \
  -verify_return_error </dev/null
```

Expected output includes:

```text
Verify return code: 0 (ok)
```

## 15.3 Test Every Broker Endpoint

Create a broker list:

```bash
cat > "${WORK_DIR}/broker-endpoints.txt" <<EOF
kafka-bootstrap.prod.example.com:9093
kafka-0.prod.example.com:9093
kafka-1.prod.example.com:9093
kafka-2.prod.example.com:9093
EOF
```

Create a validation script:

```bash
cat > "${WORK_DIR}/validate-endpoints.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ENDPOINT_FILE="${1:?Usage: $0 endpoint-file ca-bundle}"
CA_BUNDLE="${2:?Usage: $0 endpoint-file ca-bundle}"

FAILED=0

while IFS=: read -r HOST PORT; do
  [[ -z "${HOST}" ]] && continue

  echo
  echo "Testing ${HOST}:${PORT}"

  OUTPUT="$(
    openssl s_client \
      -connect "${HOST}:${PORT}" \
      -servername "${HOST}" \
      -verify_hostname "${HOST}" \
      -CAfile "${CA_BUNDLE}" \
      -verify_return_error \
      </dev/null 2>&1
  )" || true

  echo "${OUTPUT}" |
  openssl x509 \
    -noout \
    -subject \
    -issuer \
    -serial \
    -dates \
    -fingerprint \
    -sha256 2>/dev/null || true

  if ! grep -q "Verify return code: 0 (ok)" <<<"${OUTPUT}"; then
    echo "FAILED: ${HOST}:${PORT}" >&2
    FAILED=1
  else
    echo "PASSED: ${HOST}:${PORT}"
  fi
done < "${ENDPOINT_FILE}"

exit "${FAILED}"
EOF

chmod 700 "${WORK_DIR}/validate-endpoints.sh"
```

Run it:

```bash
"${WORK_DIR}/validate-endpoints.sh" \
  "${WORK_DIR}/broker-endpoints.txt" \
  "${WORK_DIR}/cacerts.pem"
```

---

# 16. Kafka Client Configuration

## 16.1 One-Way TLS Using PKCS12

Create `client.properties`:

```properties
security.protocol=SSL
ssl.truststore.location=/opt/kafka/security/kafka-client-truststore.p12
ssl.truststore.password=REPLACE_WITH_TRUSTSTORE_PASSWORD
ssl.truststore.type=PKCS12
ssl.endpoint.identification.algorithm=https
```

Keep endpoint identification enabled.

## 16.2 Mutual TLS Client Configuration

```properties
security.protocol=SSL

ssl.truststore.location=/opt/kafka/security/kafka-client-truststore.p12
ssl.truststore.password=REPLACE_WITH_TRUSTSTORE_PASSWORD
ssl.truststore.type=PKCS12

ssl.keystore.location=/opt/kafka/security/kafka-client-keystore.p12
ssl.keystore.password=REPLACE_WITH_KEYSTORE_PASSWORD
ssl.keystore.type=PKCS12
ssl.key.password=REPLACE_WITH_PRIVATE_KEY_PASSWORD

ssl.endpoint.identification.algorithm=https
```

The keystore is required when the client must present its own identity certificate. ([Confluent Documentation][5])

## 16.3 PEM-Based Java Client

Modern Kafka clients can use PEM material directly:

```properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/opt/kafka/security/cacerts.pem
ssl.endpoint.identification.algorithm=https
```

For mTLS:

```properties
security.protocol=SSL

ssl.truststore.type=PEM
ssl.truststore.location=/opt/kafka/security/cacerts.pem

ssl.keystore.type=PEM
ssl.keystore.location=/opt/kafka/security/client-fullchain.pem
ssl.key.location=/opt/kafka/security/client-key.pem
ssl.key.password=REPLACE_IF_KEY_IS_ENCRYPTED

ssl.endpoint.identification.algorithm=https
```

Confirm the supported options against the Kafka client version used by each application.

---

# 17. Deploy Client Truststore Updates in OpenShift

## 17.1 Create or Update a Client Trust Secret

```bash
export CLIENT_NAMESPACE="application-prod"
export CLIENT_TLS_SECRET="kafka-client-trust"
```

```bash
oc create secret generic "${CLIENT_TLS_SECRET}" \
  -n "${CLIENT_NAMESPACE}" \
  --from-file=kafka-client-truststore.p12="${WORK_DIR}/kafka-client-truststore.p12" \
  --from-literal=truststore-password="${CLIENT_TRUSTSTORE_PASSWORD}" \
  --dry-run=client \
  -o yaml |
oc apply -f -
```

Avoid storing plaintext passwords directly in deployment manifests or shell history. Prefer the organization's approved secret-management platform.

## 17.2 Example Deployment Mount

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-client-application
  namespace: application-prod
spec:
  template:
    spec:
      containers:
        - name: application
          image: registry.example.com/application:1.0.0
          volumeMounts:
            - name: kafka-client-trust
              mountPath: /opt/kafka/security
              readOnly: true
      volumes:
        - name: kafka-client-trust
          secret:
            secretName: kafka-client-trust
            defaultMode: 0400
```

If the application loads the truststore only during startup, restart the application after updating the secret. Kubernetes-mounted secret files can update on disk, but many JVM clients do not automatically reload an existing SSL context.

```bash
oc rollout restart deployment/kafka-client-application \
  -n "${CLIENT_NAMESPACE}"

oc rollout status deployment/kafka-client-application \
  -n "${CLIENT_NAMESPACE}"
```

---

# 18. Functional Kafka Tests

## 18.1 List Topics

```bash
kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --list
```

## 18.2 Describe Cluster Metadata

```bash
kafka-broker-api-versions \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties
```

## 18.3 Create a Temporary Test Topic

Use an approved replication factor:

```bash
export TEST_TOPIC="tls-cert-validation-$(date +%Y%m%d%H%M%S)"
```

```bash
kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --create \
  --topic "${TEST_TOPIC}" \
  --partitions 3 \
  --replication-factor 3
```

## 18.4 Produce a Test Message

```bash
TEST_MESSAGE="TLS certificate validation $(date --iso-8601=seconds)"
```

```bash
printf '%s\n' "${TEST_MESSAGE}" |
kafka-console-producer \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --producer.config client.properties \
  --topic "${TEST_TOPIC}"
```

## 18.5 Consume the Test Message

```bash
kafka-console-consumer \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --consumer.config client.properties \
  --topic "${TEST_TOPIC}" \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 30000
```

Confirm the consumed value matches the produced value.

## 18.6 Delete the Test Topic

```bash
kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --delete \
  --topic "${TEST_TOPIC}"
```

Topic deletion must be permitted by cluster policy.

---

# 19. Test Confluent Components

Validate every component that connects to Kafka:

## Kafka Connect

```bash
curl --fail --silent --show-error \
  https://connect.prod.example.com/connectors
```

Review connector status:

```bash
curl --fail --silent --show-error \
  https://connect.prod.example.com/connectors |
jq -r '.[]' |
while read -r CONNECTOR; do
  curl --fail --silent --show-error \
    "https://connect.prod.example.com/connectors/${CONNECTOR}/status"
done
```

Specifically verify Oracle and DB2 source or sink connectors remain in the `RUNNING` state.

## Schema Registry

```bash
curl --fail --silent --show-error \
  https://schema-registry.prod.example.com/subjects
```

## Control Center

Verify:

* Kafka cluster status is healthy.
* Broker metrics continue to report.
* Consumer lag continues to update.
* Connect clusters remain visible.
* No TLS handshake errors appear.

## Metadata Service

Where MDS and RBAC are enabled:

```bash
curl --fail --silent --show-error \
  https://mds.prod.example.com/security/1.0/authenticate
```

Use the approved authentication mechanism.

---

# 20. Application-Team Validation Checklist

Each application owner should validate:

* Application starts successfully.
* Bootstrap connection succeeds.
* Topic metadata can be retrieved.
* Producer messages are acknowledged.
* Consumer messages are received.
* Consumer groups remain stable.
* No `SSLHandshakeException` occurs.
* No hostname-verification failure occurs.
* No certificate-path validation error occurs.
* No authorization regression occurs.
* Application latency and error rates remain normal.

Common Java errors include:

```text
javax.net.ssl.SSLHandshakeException
PKIX path building failed
No subject alternative DNS name matching
certificate_unknown
unable to find valid certification path
```

Common librdkafka errors include:

```text
SSL handshake failed
certificate verify failed
broker certificate could not be verified
```

---

# 21. Broker and Cluster Health Validation

## 21.1 Confirm All Pods Are Ready

```bash
oc get pods \
  -n "${KAFKA_NAMESPACE}" \
  -o wide
```

No Kafka broker should remain in:

```text
CrashLoopBackOff
Error
Pending
Init:Error
0/1 Ready
```

## 21.2 Review Broker Logs

```bash
for POD in $(oc get pods \
  -n "${KAFKA_NAMESPACE}" \
  -l app=kafka \
  -o name); do

  echo "Reviewing ${POD}"

  oc logs \
    -n "${KAFKA_NAMESPACE}" \
    "${POD}" \
    --since=30m |
  grep -Ei \
    'ssl|tls|certificate|handshake|truststore|keystore|exception|error' ||
  true
done
```

Adjust the label selector to match the installed CFK resources.

## 21.3 Check Topic and Replica Health

```bash
kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --describe |
tee "${WORK_DIR}/topics-after.txt"
```

Look for:

* Offline partitions.
* Leaders with ID `-1`.
* Under-replicated partitions.
* Missing in-sync replicas.
* Unexpected leader-election churn.

## 21.4 Check Consumer Groups

```bash
kafka-consumer-groups \
  --bootstrap-server "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  --command-config client.properties \
  --all-groups \
  --describe |
tee "${WORK_DIR}/consumer-groups-after.txt"
```

---

# 22. Automated Post-Deployment Validation Script

```bash
cat > "${WORK_DIR}/post-deployment-validation.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:?Set BOOTSTRAP_SERVER}"
BOOTSTRAP_HOST="${BOOTSTRAP_HOST:?Set BOOTSTRAP_HOST}"
BOOTSTRAP_PORT="${BOOTSTRAP_PORT:?Set BOOTSTRAP_PORT}"
CLIENT_CONFIG="${CLIENT_CONFIG:?Set CLIENT_CONFIG}"
CA_FILE="${CA_FILE:?Set CA_FILE}"
EXPECTED_CERT="${EXPECTED_CERT:?Set EXPECTED_CERT}"

echo "1. Validating TLS chain and hostname"

TLS_OUTPUT="$(
  openssl s_client \
    -connect "${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}" \
    -servername "${BOOTSTRAP_HOST}" \
    -verify_hostname "${BOOTSTRAP_HOST}" \
    -CAfile "${CA_FILE}" \
    -verify_return_error \
    </dev/null 2>&1
)" || true

if ! grep -q "Verify return code: 0 (ok)" <<<"${TLS_OUTPUT}"; then
  echo "${TLS_OUTPUT}" >&2
  echo "ERROR: TLS validation failed." >&2
  exit 1
fi

echo "2. Comparing certificate fingerprints"

EXPECTED_FP="$(
  openssl x509 \
    -in "${EXPECTED_CERT}" \
    -noout \
    -fingerprint \
    -sha256
)"

PRESENTED_FP="$(
  openssl s_client \
    -connect "${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}" \
    -servername "${BOOTSTRAP_HOST}" \
    </dev/null 2>/dev/null |
  openssl x509 \
    -noout \
    -fingerprint \
    -sha256
)"

echo "Expected:  ${EXPECTED_FP}"
echo "Presented: ${PRESENTED_FP}"

if [[ "${EXPECTED_FP}" != "${PRESENTED_FP}" ]]; then
  echo "ERROR: Presented certificate does not match expected certificate." >&2
  exit 1
fi

echo "3. Listing Kafka topics"

kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "${CLIENT_CONFIG}" \
  --list >/dev/null

echo "4. Retrieving broker API versions"

kafka-broker-api-versions \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "${CLIENT_CONFIG}" >/dev/null

echo "SUCCESS: TLS and Kafka connectivity validation completed."
EOF

chmod 700 "${WORK_DIR}/post-deployment-validation.sh"
```

Run it:

```bash
export BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}"
export BOOTSTRAP_HOST="${KAFKA_BOOTSTRAP_HOST}"
export BOOTSTRAP_PORT="${KAFKA_BOOTSTRAP_PORT}"
export CLIENT_CONFIG="${WORK_DIR}/client.properties"
export CA_FILE="${WORK_DIR}/cacerts.pem"
export EXPECTED_CERT="${WORK_DIR}/server.pem"

"${WORK_DIR}/post-deployment-validation.sh"
```

---

# 23. Rollback Procedure

Rollback should be initiated when:

* Brokers fail to become ready.
* The presented certificate is incorrect.
* Clients cannot validate the new chain.
* Broker-to-broker communication fails.
* KRaft controller communication is disrupted.
* Critical production clients cannot reconnect.
* The certificate contains incorrect SAN entries.
* The certificate does not match the deployed private key.

## 23.1 Restore the Previous Secret

```bash
oc apply \
  -f "${WORK_DIR}/${TLS_SECRET_NAME}-backup.yaml"
```

## 23.2 Monitor Reconciliation

```bash
oc get pods \
  -n "${KAFKA_NAMESPACE}" \
  -w
```

```bash
oc logs \
  -n confluent \
  deployment/confluent-operator \
  --follow
```

## 23.3 Confirm the Old Certificate Is Presented Again

```bash
openssl s_client \
  -connect "${KAFKA_BOOTSTRAP_HOST}:${KAFKA_BOOTSTRAP_PORT}" \
  -servername "${KAFKA_BOOTSTRAP_HOST}" \
  </dev/null 2>/dev/null |
openssl x509 \
  -noout \
  -fingerprint \
  -sha256
```

Compare it with:

```bash
cat "${WORK_DIR}/old-certificate-fingerprint.txt"
```

## 23.4 Restore Client Truststores

Only roll back client truststores if the new CA causes a client-side issue. During a CA transition, it is preferable for client truststores to contain both the old and new CA chains until the server rollout is complete.

## 23.5 Re-run Functional Tests

Repeat:

* OpenSSL chain and hostname test.
* Topic listing.
* Broker API test.
* Producer test.
* Consumer test.
* Connector validation.
* Application smoke tests.

---

# 24. Recommended CA-Rotation Strategy

When the issuing CA changes, use an overlap process rather than replacing the trust chain in a single step.

## Phase 1: Expand Trust

Deploy truststores containing:

```text
Old root and intermediate CA
New root and intermediate CA
```

Restart clients that do not dynamically reload truststores.

## Phase 2: Rotate Server Certificate

Deploy the new Kafka server certificate signed by the new CA.

Validate all applications.

## Phase 3: Stabilization Period

Retain both CA chains for an agreed period, such as one or two release cycles.

## Phase 4: Remove Old Trust

Remove the old CA only after:

* No servers present old certificates.
* No mTLS clients use certificates signed by the old CA.
* All application owners have confirmed migration.
* Security approves retirement.

This sequence prevents a circular dependency in which the server and clients stop trusting one another during the transition.

---

# 25. Security Controls

Apply the following controls:

* Generate private keys in a restricted environment.
* Use a minimum RSA key size of 2048 bits; 3072 bits is preferred where permitted.
* Use SHA-256 or stronger signatures.
* Use SANs rather than relying on the Common Name.
* Never email or upload the private key to the CA.
* Never store unencrypted private keys in source control.
* Restrict OpenShift secret access using RBAC.
* Store backup secret YAML only in an encrypted, access-controlled location.
* Avoid putting passwords directly on command lines when command history is retained.
* Delete working copies after successful change closure.
* Record certificate serial numbers and SHA-256 fingerprints in the change record.
* Maintain alerts at least 60, 30, 14, and 7 days before expiration.

---

# 26. Production Go/No-Go Checklist

## Before Deployment

* [ ] Current Kafka cluster is healthy.
* [ ] Current certificate has been backed up.
* [ ] Kafka custom resource has been backed up.
* [ ] Private key passes validation.
* [ ] CSR contains every required SAN.
* [ ] Signed certificate matches the private key.
* [ ] Signed certificate contains every required SAN.
* [ ] Certificate validity dates are correct.
* [ ] Extended Key Usage includes server authentication.
* [ ] Certificate chain verifies successfully.
* [ ] Client-impact analysis is complete.
* [ ] New CA has been distributed where required.
* [ ] Application owners are available.
* [ ] Rollback procedure has been reviewed.

## During Deployment

* [ ] Secret update succeeds.
* [ ] CFK recognizes the change.
* [ ] Brokers roll one at a time or dynamically reload.
* [ ] Each broker returns to Ready.
* [ ] No offline partitions are created.
* [ ] No persistent under-replicated partitions occur.
* [ ] KRaft quorum remains healthy.
* [ ] Connectors remain running.
* [ ] Client connection failures remain within expected limits.

## After Deployment

* [ ] Bootstrap endpoint presents the new certificate.
* [ ] Every broker endpoint presents the new certificate.
* [ ] Certificate fingerprint matches the approved certificate.
* [ ] Certificate chain validates.
* [ ] Hostname validation succeeds.
* [ ] Topic-list operation succeeds.
* [ ] Producer test succeeds.
* [ ] Consumer test succeeds.
* [ ] Consumer groups are stable.
* [ ] Oracle connectors are running.
* [ ] DB2 connectors are running.
* [ ] Java tenant applications have been validated.
* [ ] Monitoring and alerting are normal.
* [ ] Change evidence has been captured.

---

# 27. Change Record Evidence

Retain the following evidence:

```text
kafka-cr-before.yaml
TLS secret resource version before and after
Old certificate fingerprint
New certificate fingerprint
CSR fingerprint
Certificate subject and issuer
Certificate serial number
Certificate validity dates
Certificate SAN list
OpenSSL verification output
Pod status before and after
Kafka topic and broker validation output
Connector status
Application-owner approvals
Rollback outcome, if used
```

Do not retain private-key content in the change ticket.

---

# 28. Certificate Expiration Monitoring Script

```bash
cat > "${WORK_DIR}/check-kafka-certificate-expiration.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

HOST="${1:?Usage: $0 hostname port warning-days}"
PORT="${2:?Usage: $0 hostname port warning-days}"
WARNING_DAYS="${3:-30}"

CERT_END_DATE="$(
  openssl s_client \
    -connect "${HOST}:${PORT}" \
    -servername "${HOST}" \
    </dev/null 2>/dev/null |
  openssl x509 \
    -noout \
    -enddate |
  cut -d= -f2-
)"

CERT_END_EPOCH="$(date -d "${CERT_END_DATE}" +%s)"
NOW_EPOCH="$(date +%s)"
SECONDS_REMAINING="$((CERT_END_EPOCH - NOW_EPOCH))"
DAYS_REMAINING="$((SECONDS_REMAINING / 86400))"

echo "Endpoint: ${HOST}:${PORT}"
echo "Certificate expires: ${CERT_END_DATE}"
echo "Days remaining: ${DAYS_REMAINING}"

if (( DAYS_REMAINING < 0 )); then
  echo "CRITICAL: Certificate has expired." >&2
  exit 2
elif (( DAYS_REMAINING <= WARNING_DAYS )); then
  echo "WARNING: Certificate expires within ${WARNING_DAYS} days." >&2
  exit 1
else
  echo "OK: Certificate validity is within the accepted threshold."
fi
EOF

chmod 700 "${WORK_DIR}/check-kafka-certificate-expiration.sh"
```

Example:

```bash
"${WORK_DIR}/check-kafka-certificate-expiration.sh" \
  kafka-bootstrap.prod.example.com \
  9093 \
  60
```

---

# 29. Final Operational Guidance

The safest production certificate rotation sequence is:

```text
Inventory
→ Back up
→ Generate key and CSR
→ Obtain signed certificate
→ Validate key, SANs, dates, EKU, and chain
→ Expand client trust if the CA changes
→ Update the existing OpenShift secret
→ Allow CFK to reconcile the change
→ Monitor broker health
→ Verify every TLS endpoint
→ Run producer and consumer tests
→ Validate connectors and tenant applications
→ Retain rollback material until change closure
```

The most common causes of Kafka certificate-renewal failures are:

1. Missing advertised broker hostnames in the SAN list.
2. Incorrect certificate-chain order.
3. A certificate that does not match the private key.
4. Updating the broker before distributing a new CA to clients.
5. Using incorrect key names in the OpenShift secret.
6. Restarting all brokers simultaneously.
7. Assuming applications automatically reload updated truststores.
8. Testing only the bootstrap endpoint rather than each advertised broker.
9. Disabling hostname verification instead of correcting the certificate.
10. Removing the old CA before all clients and mTLS identities have migrated.
