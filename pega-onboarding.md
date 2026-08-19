# Pega to Confluent Kafka mTLS and RBAC Onboarding Guide

## 1. Purpose

This guide defines the standard onboarding process for Pega administrators who need to connect Pega Platform applications to a Confluent Kafka environment secured with **mutual TLS (mTLS)** and **Confluent Role-Based Access Control (RBAC)**.

Pega supports Kafka configuration instances that define Kafka client connectivity, including SSL-enabled endpoints, keystore and truststore configuration, and client properties used by Pega applications and Kafka Data Sets. ([Pega Documentation][1])

The target security model is:

```text
Pega Application
       |
       | Kafka Client
       | mTLS
       v
Confluent Kafka
       |
       | Certificate Authentication
       v
Mapped Confluent Principal
       |
       | Confluent RBAC Role Bindings
       v
Kafka Topics / Consumer Groups
```

The key architectural distinction is:

```text
mTLS = Authentication
RBAC = Authorization
```

The client certificate establishes the Pega application's identity. Confluent RBAC then determines what that authenticated principal is permitted to do. Confluent role bindings can be scoped directly to Kafka topics and consumer groups. ([Confluent Documentation][2])

---

# 2. Security Architecture

Four layers must work correctly for Pega to communicate with Confluent Kafka:

```text
NETWORK CONNECTIVITY
        +
TLS TRUST
        +
mTLS AUTHENTICATION
        +
CONFLUENT RBAC AUTHORIZATION
```

### Network Connectivity

The Pega runtime environment must be able to resolve and reach the Kafka broker endpoints.

### TLS Trust

Pega must trust the Certificate Authority that issued the Kafka broker certificates.

### mTLS Authentication

Confluent Kafka must trust the Certificate Authority that issued the Pega client certificate.

### Confluent RBAC

The principal derived from the Pega certificate must have appropriate Confluent role bindings against the topics and consumer groups used by the Pega application. ([Confluent Documentation][2])

---

# 3. Target Architecture

```text
+-------------------------------------+
| Pega Platform                       |
|                                     |
| Pega Application                    |
|       |                             |
|       +-- Kafka Data Set / Client   |
|              |                      |
|              +-- Kafka Config       |
|              +-- Client Certificate |
|              +-- Private Key        |
|              +-- Truststore         |
+-----------------|-------------------+
                  |
                  | mTLS
                  v
+-------------------------------------+
| Confluent Kafka                     |
|                                     |
| TLS Listener                        |
| Certificate Authentication          |
| Principal Mapping                   |
| Confluent RBAC                      |
+-----------------|-------------------+
                  |
          +-------+--------+
          |                |
          v                v
       Topics        Consumer Groups
```

Pega represents external Kafka connectivity through Kafka configuration instances in the `Data-Admin-Kafka` class. Kafka Data Sets can then reference that configuration to interact with Kafka. ([Pega Documentation][3])

---

# 4. Information Required Before Onboarding

The Pega administrator should receive the following information from the Confluent platform team.

```text
ENVIRONMENT:
DEV

KAFKA CLUSTER:
kafka-dev

KAFKA CLUSTER ID:
<KAFKA_CLUSTER_ID>

BOOTSTRAP SERVERS:
broker1.kafka.example.com:9093
broker2.kafka.example.com:9093
broker3.kafka.example.com:9093

SECURITY:
mTLS

PEGA CERTIFICATE SUBJECT:
CN=pega-customer-events-dev,
OU=Applications,
O=Example Corporation

CONFLUENT PRINCIPAL:
User:pega-customer-events-dev

PRODUCER TOPICS:
customer.commands

CONSUMER TOPICS:
customer.events

CONSUMER GROUP:
pega-customer-events-dev

ROLE BINDINGS:

DeveloperWrite
Topic:customer.commands

DeveloperRead
Topic:customer.events

DeveloperRead
Group:pega-customer-events-dev
```

---

# 5. Pega Integration Objects

Administrators should understand the primary Pega objects involved in the integration.

A typical implementation uses:

```text
Pega Application
       |
       v
Kafka Data Set
       |
       v
Kafka Configuration
       |
       +-- Bootstrap Servers
       +-- SSL Configuration
       +-- Truststore
       +-- Keystore
       +-- Kafka Client Properties
```

Pega's Kafka Data Set connects to Kafka through a Kafka configuration rule, allowing applications to publish or consume Kafka data without embedding Kafka connection configuration directly in application logic. ([Pega Documentation][3])

---

# 6. Network Requirements

Before configuring certificates, Pega, or RBAC, verify basic connectivity.

The Pega environment must be able to:

```text
Resolve Kafka DNS
       ↓
Route to Kafka network
       ↓
Pass required firewall rules
       ↓
Connect to Kafka TLS listener
```

Example:

```text
Pega Platform
      |
      | TCP <KAFKA_TLS_PORT>
      v
Firewall
      |
      v
Confluent Kafka
```

The actual Kafka listener port must be supplied by the Confluent platform administrators.

Do not assume a port such as `9093` unless it has been explicitly documented for the environment.

---

# 7. Bootstrap Server Configuration

Configure multiple Kafka bootstrap servers whenever possible.

Example:

```text
broker1.kafka.example.com:9093,
broker2.kafka.example.com:9093,
broker3.kafka.example.com:9093
```

Kafka uses bootstrap servers only for initial cluster discovery. Once connected, the client receives metadata describing individual brokers and subsequently communicates with those brokers directly.

The Pega network must therefore be able to resolve and reach every broker that Confluent advertises.

---

# 8. DNS Validation

From the Pega runtime network, validate each broker hostname.

```bash
nslookup broker1.kafka.example.com
```

or:

```bash
dig broker1.kafka.example.com
```

Repeat for all brokers.

A common Kafka connectivity failure looks like:

```text
Pega reaches bootstrap server
        ↓
Kafka returns metadata
        ↓
Metadata contains broker hostnames
        ↓
Pega cannot resolve those hostnames
        ↓
Kafka connection fails
```

The addresses configured in Kafka's advertised listeners must therefore be resolvable and routable from the Pega environment.

---

# 9. Firewall Requirements

The required network rule should generally resemble:

```text
SOURCE:
<PEGA_RUNTIME_NETWORK>

DESTINATION:
<KAFKA_BROKER_NETWORK>

PROTOCOL:
TCP

PORT:
<KAFKA_TLS_PORT>
```

Do not permit connectivity to only one bootstrap broker unless the network architecture explicitly uses a proxy or other supported Kafka gateway.

Pega may need to communicate with any broker returned through Kafka metadata.

---

# 10. mTLS Certificate Architecture

mTLS requires both Pega and Kafka to validate the other's certificate.

```text
                    Enterprise PKI
                     /          \
                    /            \
                   v              v

        Kafka Broker Cert     Pega Client Cert
                |                    |
                v                    v

           Pega trusts        Kafka trusts
             Kafka                Pega
```

Pega therefore requires:

```text
KEYSTORE

Pega client certificate
Pega client private key
Certificate chain
```

and:

```text
TRUSTSTORE

Kafka Root CA
Kafka Intermediate CA certificates
```

Pega documentation explicitly supports SSL-enabled Kafka endpoints using keystore and truststore files. ([Pega Documentation][1])

---

# 11. Pega Client Certificate

Each Pega integration should use an application-specific certificate identity.

Example:

```text
CN=pega-order-events-prod
OU=Applications
O=Example Corporation
```

Recommended naming:

```text
CN=pega-order-events-dev
CN=pega-order-events-test
CN=pega-order-events-prod
```

Avoid identities associated with individual administrators.

Avoid:

```text
CN=JohnSmith
```

Also avoid overly generic client identities:

```text
CN=Pega
CN=KafkaClient
CN=Production
```

Application-specific identities simplify:

```text
RBAC
Audit logging
Certificate rotation
Troubleshooting
Environment isolation
Incident response
```

---

# 12. Validate the Client Certificate

Inspect the Pega certificate before importing it.

```bash
openssl x509 \
  -in pega-client.crt \
  -text \
  -noout
```

Verify:

```text
Subject
Issuer
Serial number
Validity dates
Key Usage
Extended Key Usage
Certificate chain
```

A client authentication certificate will typically include an extended key usage appropriate for TLS client authentication, subject to the organization's PKI requirements.

---

# 13. Creating the Pega Keystore

If the PKI team provides:

```text
pega-client.crt
pega-client.key
ca-chain.crt
```

create a PKCS12 file:

```bash
openssl pkcs12 \
  -export \
  -in pega-client.crt \
  -inkey pega-client.key \
  -certfile ca-chain.crt \
  -name pega-kafka-client \
  -out pega-kafka-client.p12
```

If the Pega deployment requires JKS, convert the PKCS12 keystore:

```bash
keytool \
  -importkeystore \
  -srckeystore pega-kafka-client.p12 \
  -srcstoretype PKCS12 \
  -destkeystore pega-kafka-client.jks \
  -deststoretype JKS
```

Inspect:

```bash
keytool \
  -list \
  -v \
  -keystore pega-kafka-client.jks
```

Verify that the entry containing the Pega certificate is a:

```text
PrivateKeyEntry
```

The client certificate alone is insufficient for mTLS; Pega must have access to its associated private key.

---

# 14. Creating the Kafka Truststore

Assume the Kafka team provides:

```text
enterprise-kafka-root-ca.crt
```

Create a JKS truststore:

```bash
keytool \
  -importcert \
  -alias kafka-root-ca \
  -file enterprise-kafka-root-ca.crt \
  -keystore kafka-truststore.jks
```

If an intermediate CA is required:

```bash
keytool \
  -importcert \
  -alias kafka-intermediate-ca \
  -file kafka-intermediate-ca.crt \
  -keystore kafka-truststore.jks
```

Inspect:

```bash
keytool \
  -list \
  -v \
  -keystore kafka-truststore.jks
```

Pega's Kafka configuration supports selecting uploaded truststore and keystore files when SSL is enabled. ([Pega Documentation][1])

---

# 15. Test the Kafka TLS Endpoint

From a host with the same network path as the Pega environment:

```bash
openssl s_client \
  -connect broker1.kafka.example.com:9093 \
  -servername broker1.kafka.example.com \
  -showcerts
```

This validates:

```text
DNS
TCP connectivity
TLS listener
Kafka broker certificate
Certificate chain
```

To validate mTLS using the Pega identity:

```bash
openssl s_client \
  -connect broker1.kafka.example.com:9093 \
  -servername broker1.kafka.example.com \
  -cert pega-client.crt \
  -key pega-client.key \
  -CAfile kafka-ca.crt
```

A successful OpenSSL TLS handshake proves the TLS layer works but does **not** prove that the principal has sufficient Confluent RBAC permissions.

---

# 16. Upload the Keystore and Truststore into Pega

Pega Kafka configuration supports uploaded keystore and truststore objects for SSL-enabled Kafka connections. ([Pega Documentation][1])

The administrative process generally follows:

```text
Pega Dev Studio
      |
      v
Kafka Configuration
      |
      v
Security Settings
      |
      +-- Enable SSL
      |
      +-- Truststore
      |
      +-- Keystore
```

Depending upon the Pega deployment model, certificate material may also be managed through external secrets integration rather than embedding credentials directly in application configuration. Pega documents support for mapping external Kafka keystore and truststore information through external secret stores in supported environments. ([Pega Documentation][4])

---

# 17. Create the Kafka Configuration Instance

Create a Kafka configuration instance in Pega.

Conceptually:

```text
Records
   ↓
SysAdmin
   ↓
Kafka
```

or the corresponding administration path for the deployed Pega version.

The Kafka configuration rule belongs to:

```text
Data-Admin-Kafka
```

and contains the Kafka client configuration used by Kafka Data Sets. ([Pega Documentation][3])

Populate:

```text
NAME:
Confluent-Kafka-Prod

BOOTSTRAP SERVERS:
broker1.kafka.example.com:9093
broker2.kafka.example.com:9093
broker3.kafka.example.com:9093

USE SSL:
Enabled

TRUSTSTORE:
kafka-truststore.jks

KEYSTORE:
pega-kafka-client.jks
```

Use the exact configuration fields and administrative screens appropriate for the deployed Pega Platform version. Pega's current documentation specifically exposes SSL configuration, keystore, and truststore settings on Kafka configuration instances. ([Pega Documentation][5])

---

# 18. Kafka Data Set Configuration

Create or update the Kafka Data Set that will use the connection.

Conceptually:

```text
Pega Application
      |
      v
Kafka Data Set
      |
      +-- Kafka Configuration
      |
      +-- Topic
      |
      +-- Serialization
      |
      +-- Consumer / Producer behavior
```

A Kafka Data Set can connect to Kafka by referencing the previously created Kafka configuration instance. ([Pega Documentation][3])

Example:

```text
DATA SET:
OrderEvents

KAFKA CONFIGURATION:
Confluent-Kafka-Prod

TOPIC:
orders.events
```

---

# 19. Understanding the Confluent Principal

mTLS authenticates Pega using the client certificate subject.

Example certificate:

```text
Subject:

CN=pega-order-events-prod,
OU=Applications,
O=Example Corporation
```

Confluent can apply TLS principal mapping rules to convert the certificate distinguished name into the Kafka principal used by RBAC.

For example:

```text
Certificate DN:

CN=pega-order-events-prod,
OU=Applications,
O=Example Corporation
```

may become:

```text
User:pega-order-events-prod
```

The Confluent platform team should document both:

```text
CERTIFICATE SUBJECT:
CN=pega-order-events-prod,
OU=Applications,
O=Example Corporation

MAPPED PRINCIPAL:
User:pega-order-events-prod
```

The **mapped principal**, not an assumed certificate CN, must be used when creating role bindings.

---

# 20. Confluent RBAC Model

Confluent RBAC combines:

```text
PRINCIPAL
      +
ROLE
      +
RESOURCE
```

For example:

```text
User:pega-order-events-prod
           |
           v
    DeveloperWrite
           |
           v
 Topic:orders.commands
```

Confluent supports predefined roles and resource-scoped bindings against resources including Kafka topics and consumer groups. ([Confluent Documentation][2])

---

# 21. Recommended Pega Roles

For normal Pega Kafka integrations, the most commonly applicable roles are:

```text
DeveloperWrite
DeveloperRead
```

The basic model is:

```text
PRODUCER

Pega Principal
      |
DeveloperWrite
      |
      v
Kafka Topic
```

and:

```text
CONSUMER

Pega Principal
      |
      +--> DeveloperRead --> Topic
      |
      +--> DeveloperRead --> Consumer Group
```

Confluent documents `DeveloperWrite` for producing data and `DeveloperRead` for consuming data. ([Confluent Documentation][6])

---

# 22. Producer Role Binding

Assume:

```text
Principal:
User:pega-order-events-prod

Topic:
orders.commands
```

Create:

```text
Principal:
User:pega-order-events-prod

Role:
DeveloperWrite

Resource:
Topic:orders.commands
```

Representative CLI command:

```bash
confluent iam rbac role-binding create \
  --principal User:pega-order-events-prod \
  --role DeveloperWrite \
  --resource Topic:orders.commands \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

The exact command syntax should be validated against the Confluent CLI version installed in the environment.

---

# 23. Consumer Topic Role Binding

Assume Pega consumes:

```text
orders.events
```

Create:

```text
Principal:
User:pega-order-events-prod

Role:
DeveloperRead

Resource:
Topic:orders.events
```

Representative command:

```bash
confluent iam rbac role-binding create \
  --principal User:pega-order-events-prod \
  --role DeveloperRead \
  --resource Topic:orders.events \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

---

# 24. Consumer Group Role Binding

If the Pega consumer uses:

```text
pega-order-events-prod
```

as its consumer group, create:

```text
Principal:
User:pega-order-events-prod

Role:
DeveloperRead

Resource:
Group:pega-order-events-prod
```

Representative command:

```bash
confluent iam rbac role-binding create \
  --principal User:pega-order-events-prod \
  --role DeveloperRead \
  --resource Group:pega-order-events-prod \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

Confluent permits role bindings to be scoped independently to topics and consumer groups. ([Confluent Documentation][2])

---

# 25. Complete Producer Example

```text
Pega Application:
Order Processing

Principal:
User:pega-order-events-prod

Topic:
orders.commands
```

Authorization:

```text
User:pega-order-events-prod
            |
            v
     DeveloperWrite
            |
            v
 Topic:orders.commands
```

---

# 26. Complete Consumer Example

Suppose Pega consumes:

```text
Topic:
orders.events

Consumer Group:
pega-order-events-prod
```

Authorization:

```text
User:pega-order-events-prod
          |
          +---------------------------+
          |                           |
          v                           v
   DeveloperRead               DeveloperRead
          |                           |
          v                           v
Topic:orders.events   Group:pega-order-events-prod
```

Both bindings should be included in the onboarding request.

---

# 27. Full Pega RBAC Example

A typical bidirectional Pega integration might have:

```text
PRINCIPAL

User:pega-order-events-prod
```

with:

```text
PRODUCER

DeveloperWrite
       |
       v
Topic:orders.commands
```

and:

```text
CONSUMER

DeveloperRead
       |
       +--> Topic:orders.events
       |
       +--> Group:pega-order-events-prod
```

---

# 28. Principle of Least Privilege

Role bindings should be applied at the narrowest practical resource scope.

Prefer:

```text
DeveloperWrite
Topic:orders.commands
```

rather than:

```text
DeveloperWrite
KafkaCluster
```

if Pega only needs access to one producer topic.

Likewise, prefer:

```text
DeveloperRead
Topic:orders.events

DeveloperRead
Group:pega-order-events-prod
```

instead of broad cluster-level read authorization.

Confluent resource-level bindings are specifically intended to restrict a role to identified Kafka resources. ([Confluent Documentation][2])

---

# 29. Environment Separation

Each environment should use a distinct client certificate and principal.

```text
DEV

CN=pega-order-events-dev
User:pega-order-events-dev
```

```text
TEST

CN=pega-order-events-test
User:pega-order-events-test
```

```text
PROD

CN=pega-order-events-prod
User:pega-order-events-prod
```

Role bindings should then remain environment-specific:

```text
DEV

User:pega-order-events-dev
    |
    +-- DeveloperWrite → Topic:dev.orders.commands
    +-- DeveloperRead  → Topic:dev.orders.events
    +-- DeveloperRead  → Group:pega-order-events-dev
```

Production:

```text
PROD

User:pega-order-events-prod
    |
    +-- DeveloperWrite → Topic:orders.commands
    +-- DeveloperRead  → Topic:orders.events
    +-- DeveloperRead  → Group:pega-order-events-prod
```

---

# 30. Verify Role Bindings

Before troubleshooting Pega, verify the RBAC assignments associated with the principal.

Representative command:

```bash
confluent iam rbac role-binding list \
  --principal User:pega-order-events-prod \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

Review:

```text
Principal
Role
Resource
Kafka Cluster
```

The expected result should conceptually show:

```text
User:pega-order-events-prod

DeveloperWrite
Topic:orders.commands

DeveloperRead
Topic:orders.events

DeveloperRead
Group:pega-order-events-prod
```

---

# 31. Producer Validation

Create a controlled producer test in Pega.

Example payload:

```json
{
  "source": "pega",
  "application": "order-processing",
  "type": "kafka-connectivity-test"
}
```

Test path:

```text
Pega Application
       |
       v
Kafka Data Set
       |
       v
orders.commands
```

Validate:

```text
Network connection succeeds
TLS handshake succeeds
Client certificate is accepted
Principal mapping is correct
DeveloperWrite binding succeeds
Kafka receives the message
```

---

# 32. Consumer Validation

Publish a known test record to:

```text
orders.events
```

Then verify:

```text
orders.events
      |
      v
Kafka Data Set
      |
      v
Pega Application
```

Validate:

```text
Pega receives the message
Correct consumer group is used
Offset is committed
No authorization errors occur
```

The principal must have both:

```text
DeveloperRead → Topic
```

and:

```text
DeveloperRead → Consumer Group
```

for the intended resources.

---

# 33. Recommended Troubleshooting Sequence

Troubleshoot from the bottom of the architecture upward.

```text
1. DNS
      ↓
2. Routing
      ↓
3. Firewall
      ↓
4. Kafka listener
      ↓
5. Broker certificate
      ↓
6. Pega truststore
      ↓
7. Pega client keystore
      ↓
8. Client certificate
      ↓
9. mTLS handshake
      ↓
10. Principal mapping
      ↓
11. Confluent role bindings
      ↓
12. Topic authorization
      ↓
13. Consumer group authorization
      ↓
14. Pega Kafka Data Set
      ↓
15. Application processing
```

This sequence prevents administrators from troubleshooting Pega Data Sets or RBAC while the actual problem is DNS, routing, or PKI.

---

# 34. Troubleshooting: PKIX Path Building Failed

Example:

```text
PKIX path building failed
unable to find valid certification path
```

Likely cause:

```text
Pega does not trust the Kafka server certificate.
```

Verify:

```text
Kafka certificate
Intermediate CA
Root CA
Pega Kafka truststore
```

Inspect:

```bash
keytool \
  -list \
  -v \
  -keystore kafka-truststore.jks
```

---

# 35. Troubleshooting: Kafka Rejects Client Certificate

Possible symptoms include:

```text
SSLHandshakeException
certificate_unknown
bad_certificate
```

Possible causes:

```text
Kafka does not trust the Pega certificate CA
Incorrect certificate loaded
Incomplete certificate chain
Certificate expired
Certificate not yet valid
Keystore does not contain private key
```

Inspect:

```bash
openssl x509 \
  -in pega-client.crt \
  -text \
  -noout
```

and:

```bash
keytool \
  -list \
  -v \
  -keystore pega-kafka-client.jks
```

---

# 36. Troubleshooting: Hostname Verification

Example:

```text
No subject alternative DNS name matching
broker1.kafka.example.com
```

Verify that the broker certificate contains:

```text
Subject Alternative Name:

DNS:broker1.kafka.example.com
```

The correct solution is to fix the certificate, hostname, or advertised listener configuration.

Do not permanently disable hostname verification as a workaround.

---

# 37. Troubleshooting: Authentication Works but Pega Cannot Produce

If mTLS succeeds but Pega cannot produce:

```text
Pega Certificate
       ↓
Principal Mapping
       ↓
Confluent Principal
       ↓
DeveloperWrite
       ↓
Topic
```

Check:

```text
Expected certificate DN
Actual certificate DN
Mapped Confluent principal
Topic name
Kafka cluster ID
DeveloperWrite binding
```

For example:

```text
User:pega-order-events-prod
       |
       v
DeveloperWrite
       |
       v
Topic:orders.commands
```

---

# 38. Troubleshooting: Pega Cannot Consume

For consumer problems, validate both resources:

```text
User:pega-order-events-prod
      |
      +-- DeveloperRead → Topic:orders.events
      |
      +-- DeveloperRead → Group:pega-order-events-prod
```

A Pega consumer can have permission to read a topic while still being unable to use the configured consumer group.

---

# 39. Troubleshooting: Incorrect Principal Mapping

Suppose the expected principal is:

```text
User:pega-order-events-prod
```

but Kafka authenticates:

```text
User:CN=pega-order-events-prod,
OU=Applications,
O=Example Corporation
```

Role bindings attached to the simplified principal will not apply to the full DN principal.

Verify the configured Confluent/Kafka TLS principal mapping rules and determine the actual principal generated during authentication.

The correct troubleshooting sequence is:

```text
Certificate Subject
       ↓
Principal Mapping Rule
       ↓
Actual Kafka Principal
       ↓
Confluent Role Binding
```

---

# 40. Troubleshooting: Bootstrap Connection Works but Kafka Fails

This commonly indicates a metadata-routing issue.

Example:

```text
Pega
 |
 | connects
 v
bootstrap.kafka.example.com
 |
 | Kafka metadata
 v
broker01.internal.example.com
 |
 | DNS or routing failure
 X
Pega
```

Verify:

```text
advertised.listeners
Broker DNS
Network routes
Firewall rules
```

---

# 41. Certificate Rotation

Certificate rotation should preserve the logical Pega identity whenever possible.

Old certificate:

```text
CN=pega-order-events-prod
```

Replacement certificate:

```text
CN=pega-order-events-prod
```

Mapped principal:

```text
User:pega-order-events-prod
```

If the mapped principal does not change, the existing Confluent role bindings can continue to represent the application.

Recommended workflow:

```text
Issue replacement certificate
        ↓
Verify certificate subject
        ↓
Verify principal mapping
        ↓
Confirm existing role bindings
        ↓
Create updated Pega keystore
        ↓
Deploy to lower environment
        ↓
Test produce/consume
        ↓
Deploy to production
        ↓
Validate connectivity
        ↓
Retire previous certificate
```

---

# 42. Certificate Expiration Monitoring

Track:

```text
Application
Environment
Certificate subject
Certificate serial number
Issuer
Issue date
Expiration date
Mapped principal
Keystore
Kafka configuration
Topics
Consumer groups
Application owner
Technical owner
```

Certificate renewal should begin sufficiently before expiration to allow time for PKI issuance, lower-environment testing, production deployment, and rollback.

---

# 43. Pega Onboarding Request Template

The Pega team should submit:

```text
APPLICATION:
<PEGA_APPLICATION>

PEGA RULESET:
<RULESET>

APPLICATION OWNER:
<OWNER>

TECHNICAL CONTACT:
<CONTACT>

ENVIRONMENT:
<DEV | TEST | PROD>

PEGA SOURCE NETWORK:
<CIDR_OR_NETWORK>

CERTIFICATE SUBJECT:
<PEGA_CERTIFICATE_DN>

KAFKA CONFIGURATION NAME:
<PEGA_KAFKA_CONFIG>

PRODUCER TOPICS:
<TOPICS>

CONSUMER TOPICS:
<TOPICS>

CONSUMER GROUPS:
<GROUPS>

EXPECTED MESSAGE RATE:
<MESSAGES_PER_SECOND>

AVERAGE MESSAGE SIZE:
<SIZE>

MAXIMUM MESSAGE SIZE:
<SIZE>

DATA CLASSIFICATION:
<CLASSIFICATION>
```

---

# 44. Confluent Platform Response Template

The Confluent team should return:

```text
ENVIRONMENT:
<ENVIRONMENT>

KAFKA CLUSTER:
<NAME>

KAFKA CLUSTER ID:
<CLUSTER_ID>

BOOTSTRAP SERVERS:
<BROKER1:PORT,BROKER2:PORT,BROKER3:PORT>

AUTHENTICATION:
mTLS

SERVER CA:
<CA_INFORMATION>

PEGA CERTIFICATE DN:
<CERTIFICATE_DN>

MAPPED PRINCIPAL:
User:<PEGA_PRINCIPAL>

ROLE BINDINGS:

DeveloperWrite
Topic:<PRODUCER_TOPIC>

DeveloperRead
Topic:<CONSUMER_TOPIC>

DeveloperRead
Group:<CONSUMER_GROUP>

SUPPORT TEAM:
<TEAM>

PRODUCTION SUPPORT CONTACT:
<CONTACT>
```

---

# 45. Responsibility Matrix

| Activity                       | Pega Team | Confluent Team | Network Team | PKI Team |
| ------------------------------ | --------: | -------------: | -----------: | -------: |
| Pega application configuration |         X |                |              |          |
| Kafka configuration rule       |         X |                |              |          |
| Kafka Data Set                 |         X |                |              |          |
| Pega keystore configuration    |         X |                |              |          |
| Pega truststore configuration  |         X |                |              |          |
| Kafka topic provisioning       |           |              X |              |          |
| Consumer group design          |         X |              X |              |          |
| Certificate principal mapping  |           |              X |              |          |
| Confluent role bindings        |           |              X |              |          |
| Client certificate request     |         X |                |              |        X |
| Client certificate issuance    |           |                |              |        X |
| Kafka server certificates      |           |              X |              |        X |
| DNS                            |           |                |            X |          |
| Firewall                       |           |                |            X |          |
| Routing                        |           |                |            X |          |
| Producer validation            |         X |              X |              |          |
| Consumer validation            |         X |              X |              |          |
| End-to-end testing             |         X |              X |            X |          |

---

# 46. Production Readiness Checklist

* [ ] Pega can resolve every required Kafka broker.
* [ ] Pega can reach the Kafka TLS listener.
* [ ] Firewall changes are complete.
* [ ] Kafka advertised broker addresses are accessible.
* [ ] Kafka broker certificates are valid.
* [ ] Broker certificates contain appropriate SAN entries.
* [ ] Pega truststore contains the Kafka CA chain.
* [ ] Pega keystore contains the client certificate.
* [ ] Pega keystore contains the corresponding private key.
* [ ] Client certificate is valid and unexpired.
* [ ] Kafka trusts the client certificate CA.
* [ ] mTLS handshake succeeds.
* [ ] Pega Kafka configuration is created.
* [ ] SSL is enabled on the Kafka configuration.
* [ ] Correct keystore is selected.
* [ ] Correct truststore is selected.
* [ ] Correct bootstrap servers are configured.
* [ ] Certificate DN maps to the expected Confluent principal.
* [ ] `DeveloperWrite` is bound to required producer topics.
* [ ] `DeveloperRead` is bound to required consumer topics.
* [ ] `DeveloperRead` is bound to required consumer groups.
* [ ] No unnecessary broad cluster bindings exist.
* [ ] Kafka Data Set points to the correct Kafka configuration.
* [ ] Producer validation succeeds.
* [ ] Consumer validation succeeds.
* [ ] Certificate expiration monitoring exists.
* [ ] Application ownership is documented.
* [ ] Production support ownership is documented.
* [ ] Secrets are stored in an approved mechanism.

---

# 47. Recommended Enterprise Onboarding Workflow

```text
Pega Application Team
        |
        | Submit Kafka onboarding request
        v
Confluent Platform Team
        |
        | Validate topics and groups
        v
PKI Team
        |
        | Issue Pega client certificate
        v
Confluent Platform Team
        |
        | Validate certificate DN
        | Validate principal mapping
        v
Network Team
        |
        | Configure routing
        | Configure firewall
        | Validate DNS
        v
Confluent Platform Team
        |
        | DeveloperWrite → Producer Topics
        | DeveloperRead  → Consumer Topics
        | DeveloperRead  → Consumer Groups
        v
Pega Team
        |
        | Build/import keystore
        | Build/import truststore
        | Create Kafka configuration
        | Enable SSL
        | Configure Kafka Data Set
        v
Integration Testing
        |
        | Producer test
        | Consumer test
        v
Production Approval
```

---

# 48. Final Security Relationship

Administrators should always be able to trace the complete identity and authorization chain:

```text
PEGA APPLICATION
       |
       v
CLIENT CERTIFICATE
       |
       v
CERTIFICATE SUBJECT
       |
       v
TLS PRINCIPAL MAPPING
       |
       v
CONFLUENT PRINCIPAL
       |
       v
RBAC ROLE BINDING
       |
       +------------------------+
       |                        |
       v                        v
    KAFKA TOPIC          CONSUMER GROUP
```

For example:

```text
Pega Application
Order Processing
        |
        v
CN=pega-order-events-prod
        |
        v
User:pega-order-events-prod
        |
        +-----------------------------+
        |              |              |
        v              v              v
DeveloperWrite   DeveloperRead   DeveloperRead
        |              |              |
        v              v              v
orders.commands orders.events   pega-order-events-prod
```

This model provides a clean separation of responsibilities:

```text
NETWORK
Determines whether Pega can reach Kafka.

PKI / mTLS
Determines whether Pega and Kafka trust one another.

PRINCIPAL MAPPING
Determines the identity Kafka assigns to the Pega client.

CONFLUENT RBAC
Determines which Kafka resources that identity may access.

PEGA CONFIGURATION
Determines how the application uses Kafka.
```

For Pega administrators, a successful TLS connection only demonstrates that the connectivity and authentication layers are operating. The application will not successfully publish or consume until the mapped Confluent principal also has the appropriate resource-scoped `DeveloperWrite` and `DeveloperRead` role bindings for the topics and consumer groups it uses. ([Confluent Documentation][2])

[1]: https://docs.pega.com/bundle/platform/page/platform/decision-management/create-data-admin-kafka.html?utm_source=chatgpt.com "Creating a Kafka configuration instance"
[2]: https://docs.confluent.io/platform/current/security/authorization/rbac/rbac-predefined-roles.html?utm_source=chatgpt.com "Use Predefined RBAC Roles in Confluent Platform"
[3]: https://docs.pega.com/bundle/platform/page/platform/decision-management/kafka-data-set.html?utm_source=chatgpt.com "Connecting to Kafka through a Kafka Data Set"
[4]: https://docs.pega.com/bundle/pega-cloud/page/platform/security/identity-federation-and-external-secret-stores.html?utm_source=chatgpt.com "Identity federation and external secret stores"
[5]: https://docs.pega.com/bundle/blueprint/page/platform/decision-management/create-data-admin-kafka.html?utm_source=chatgpt.com "Creating a Kafka configuration instance"
[6]: https://docs.confluent.io/cloud/current/security/access-control/rbac/manage-role-bindings.html?utm_source=chatgpt.com "Manage RBAC role bindings in Confluent Cloud"
