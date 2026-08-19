# MuleSoft to Confluent Kafka mTLS and RBAC Onboarding Guide

## 1. Purpose

This guide provides the standard onboarding process for MuleSoft administrators who need to connect a Mule application to a Confluent Kafka environment secured with **mutual TLS (mTLS)** and **Confluent Role-Based Access Control (RBAC)**.

The security model is:

```text
MuleSoft Application
        |
        | mTLS
        v
Confluent Kafka
        |
        | Certificate authentication
        v
Mapped Confluent Principal
        |
        | Role Bindings
        v
Topics / Consumer Groups
```

mTLS establishes the identity of the MuleSoft application. Confluent then uses that authenticated principal when evaluating RBAC role bindings. Confluent supports assigning predefined roles at resource scope, including Kafka topics and consumer groups. ([Confluent Documentation][1])

---

# 2. Security Model

There are four separate layers involved in the connection:

```text
NETWORK CONNECTIVITY
        +
TLS TRUST
        +
mTLS AUTHENTICATION
        +
CONFLUENT RBAC AUTHORIZATION
```

Each layer must operate correctly before the MuleSoft application can produce or consume Kafka records.

### Network Connectivity

MuleSoft must be able to resolve and reach the Kafka brokers.

### TLS Trust

MuleSoft must trust the Certificate Authority that issued the Kafka broker certificates.

### mTLS Authentication

Kafka must trust the Certificate Authority that issued the MuleSoft client certificate.

### RBAC Authorization

The principal derived from the MuleSoft client certificate must have the appropriate **Confluent role bindings** for the topics and consumer groups that the application uses.

---

# 3. Target Architecture

```text
+--------------------------------+
| MuleSoft Runtime               |
|                                |
| Mule Application               |
|      |                         |
|      +-- Kafka Connector       |
|           |                    |
|           +-- Client Cert      |
|           +-- Private Key      |
|           +-- Truststore       |
+---------------|----------------+
                |
                | mTLS
                v
+--------------------------------+
| Confluent Kafka                |
|                                |
| TLS Listener                   |
| Client Certificate Validation  |
| Principal Mapping              |
| Confluent RBAC                 |
+---------------|----------------+
                |
          +-----+------+
          |            |
          v            v
       Topics      Consumer Groups
```

The MuleSoft Kafka client authenticates using its X.509 certificate. Confluent can map the X.500 distinguished name from that certificate into a simpler principal used by the authorization system. ([Confluent Documentation][2])

---

# 4. Information Required Before Onboarding

The MuleSoft administrator should receive an onboarding package from the Confluent platform team.

Example:

```text
ENVIRONMENT:
DEV

KAFKA CLUSTER:
kafka-dev

BOOTSTRAP SERVERS:
broker1.kafka.example.com:9093
broker2.kafka.example.com:9093
broker3.kafka.example.com:9093

SECURITY:
mTLS

MULESOFT CERTIFICATE SUBJECT:
CN=mulesoft-customer-api-dev,
OU=Applications,
O=Example Corporation

CONFLUENT PRINCIPAL:
User:mulesoft-customer-api-dev

PRODUCER TOPICS:
customer.commands

CONSUMER TOPICS:
customer.events

CONSUMER GROUP:
mulesoft-customer-api-dev

REQUIRED ROLE BINDINGS:
DeveloperWrite → Topic:customer.commands
DeveloperRead → Topic:customer.events
DeveloperRead → Group:mulesoft-customer-api-dev
```

---

# 5. Network Requirements

Before evaluating certificates or RBAC, establish network connectivity.

The MuleSoft runtime must be able to:

```text
Resolve broker DNS
        ↓
Route to Kafka network
        ↓
Pass firewall controls
        ↓
Reach Kafka TLS listener
```

Example:

```text
MuleSoft Runtime
      |
      | TCP 9093
      v
Firewall
      |
      v
Confluent Kafka
```

The actual listener port must be obtained from the Confluent platform team.

Do not assume that every environment uses `9093`.

---

# 6. Kafka Bootstrap Servers

Configure multiple bootstrap servers where possible.

For example:

```text
broker1.kafka.example.com:9093,
broker2.kafka.example.com:9093,
broker3.kafka.example.com:9093
```

The bootstrap server is only the initial entry point.

After establishing the initial connection, the Kafka client receives cluster metadata containing broker addresses.

Therefore MuleSoft must also be able to reach the broker addresses advertised by Kafka.

---

# 7. DNS Requirements

Test broker resolution from the network where the Mule runtime executes.

```bash
nslookup broker1.kafka.example.com
```

or:

```bash
dig broker1.kafka.example.com
```

Repeat for every broker returned by the Kafka environment.

A common failure pattern is:

```text
Bootstrap address reachable
        ↓
Kafka metadata returned
        ↓
Advertised broker address received
        ↓
Broker hostname cannot be resolved
        ↓
Kafka client fails
```

The Confluent platform team must therefore ensure that `advertised.listeners` contains addresses usable from the MuleSoft environment.

---

# 8. Firewall Requirements

The MuleSoft runtime should be permitted to initiate TCP connections to the Kafka brokers.

Example request:

```text
SOURCE:
<MULESOFT_SOURCE_NETWORK>

DESTINATION:
<KAFKA_BROKER_NETWORK>

PROTOCOL:
TCP

PORT:
<KAFKA_TLS_PORT>
```

If MuleSoft is outside the Kafka network, firewall rules must account for all broker endpoints potentially returned by Kafka metadata.

---

# 9. mTLS Certificate Architecture

mTLS requires both systems to establish trust.

```text
                Enterprise PKI
                   /       \
                  /         \
                 v           v

        Kafka Broker Cert   MuleSoft Client Cert
                |                   |
                v                   v

        MuleSoft trusts       Kafka trusts
        Kafka certificate     MuleSoft certificate
```

The MuleSoft application therefore needs both:

```text
KEYSTORE
Client certificate
Private key
Certificate chain
```

and:

```text
TRUSTSTORE
Root CA
Intermediate CAs
```

Mule supports explicit key store and trust store configuration through its TLS subsystem. ([MuleSoft Documentation][3])

---

# 10. MuleSoft Client Certificate

Use a certificate specifically assigned to the MuleSoft application.

For example:

```text
CN=mulesoft-order-api-prod
OU=Applications
O=Example Corporation
```

Avoid identities tied to individual administrators.

For example, avoid:

```text
CN=JohnSmith
```

or generic identities such as:

```text
CN=MuleSoft
```

Instead use application-specific identities:

```text
CN=mulesoft-order-api-dev
CN=mulesoft-order-api-test
CN=mulesoft-order-api-prod
```

This makes RBAC authorization and certificate lifecycle management substantially easier.

---

# 11. Creating the MuleSoft Keystore

If PKI provides:

```text
mulesoft-client.crt
mulesoft-client.key
ca-chain.crt
```

create a PKCS12 keystore:

```bash
openssl pkcs12 \
  -export \
  -in mulesoft-client.crt \
  -inkey mulesoft-client.key \
  -certfile ca-chain.crt \
  -name mulesoft-kafka \
  -out mulesoft-kafka.p12
```

Inspect the keystore:

```bash
keytool \
  -list \
  -v \
  -keystore mulesoft-kafka.p12 \
  -storetype PKCS12
```

Verify that the client identity appears as:

```text
PrivateKeyEntry
```

The application requires both the certificate and its corresponding private key to perform mTLS authentication.

---

# 12. Creating the MuleSoft Kafka Truststore

Assume the Kafka platform provides:

```text
enterprise-root-ca.crt
```

Create the truststore:

```bash
keytool \
  -importcert \
  -alias kafka-root-ca \
  -file enterprise-root-ca.crt \
  -keystore kafka-truststore.p12 \
  -storetype PKCS12
```

Verify:

```bash
keytool \
  -list \
  -v \
  -keystore kafka-truststore.p12 \
  -storetype PKCS12
```

Include intermediate CAs when required by the organization's PKI hierarchy.

---

# 13. Validate the MuleSoft Client Certificate

Inspect the certificate:

```bash
openssl x509 \
  -in mulesoft-client.crt \
  -text \
  -noout
```

Verify:

```text
Subject
Issuer
Validity
Key Usage
Extended Key Usage
Certificate Chain
```

The exact certificate policy depends on the organization's PKI standards, but a client certificate commonly includes:

```text
TLS Web Client Authentication
```

---

# 14. Test the Kafka TLS Endpoint

From a system that has network connectivity to the Kafka environment:

```bash
openssl s_client \
  -connect broker1.kafka.example.com:9093 \
  -servername broker1.kafka.example.com \
  -showcerts
```

This verifies:

```text
DNS
TCP connectivity
TLS endpoint
Kafka server certificate
```

To test using the MuleSoft client identity:

```bash
openssl s_client \
  -connect broker1.kafka.example.com:9093 \
  -servername broker1.kafka.example.com \
  -cert mulesoft-client.crt \
  -key mulesoft-client.key \
  -CAfile kafka-ca.crt
```

This is useful for distinguishing TLS problems from later Kafka authorization problems.

---

# 15. MuleSoft Kafka TLS Configuration

The MuleSoft Kafka Connector should be configured with:

```text
Bootstrap servers
Truststore
Keystore
Client certificate
Private key
Producer configuration
Consumer configuration
```

A representative TLS context may resemble:

```xml
<tls:context name="Kafka_TLS_Context">

    <tls:trust-store
        path="kafka-truststore.p12"
        password="${kafka.truststore.password}"
        type="PKCS12"/>

    <tls:key-store
        path="mulesoft-kafka.p12"
        type="PKCS12"
        alias="mulesoft-kafka"
        keyPassword="${kafka.keystore.password}"
        password="${kafka.keystore.password}"/>

</tls:context>
```

The exact generated Mule XML can vary according to Mule Runtime and connector version.

Secrets should not be stored directly in application source.

Use an approved mechanism such as:

```text
Mule Secure Configuration Properties
Anypoint Secrets Manager
Environment variables
Enterprise secrets management platform
```

---

# 16. Understanding the Confluent Principal

The certificate identity and the Confluent principal are closely related but may not be identical.

Suppose the MuleSoft certificate contains:

```text
CN=mulesoft-order-api-prod,
OU=Applications,
O=Example Corporation
```

Kafka could initially identify the client by its certificate distinguished name.

Confluent principal mapping can transform that DN into a simpler identity. ([Confluent Documentation][2])

For example:

```text
Certificate DN

CN=mulesoft-order-api-prod,
OU=Applications,
O=Example Corporation
```

becomes:

```text
User:mulesoft-order-api-prod
```

This mapped identity should be the principal referenced by Confluent role bindings.

The platform team should explicitly tell the MuleSoft administrator:

```text
CERTIFICATE SUBJECT:
CN=mulesoft-order-api-prod,...

MAPPED PRINCIPAL:
User:mulesoft-order-api-prod
```

---

# 17. Confluent RBAC

Confluent RBAC grants permissions by creating **role bindings** between:

```text
PRINCIPAL
      +
ROLE
      +
RESOURCE
```

For example:

```text
User:mulesoft-order-api-prod
        |
        +-- DeveloperWrite
        |
        +-- Topic:orders.commands
```

Confluent provides predefined roles that can be bound at the Kafka cluster or individual resource level. Supported resource scopes include topics and consumer groups. ([Confluent Documentation][1])

---

# 18. Recommended MuleSoft Roles

The two roles that will generally be most important for application onboarding are:

```text
DeveloperRead
DeveloperWrite
```

Their usage should correspond to application behavior.

A producer typically needs:

```text
DeveloperWrite
        |
        v
Topic
```

A consumer typically needs:

```text
DeveloperRead
        |
        +--> Topic
        |
        +--> Consumer Group
```

The platform team should always verify the precise Confluent role semantics against the version deployed in the environment.

---

# 19. Producer Role Binding

Assume:

```text
Principal:
User:mulesoft-order-api-prod

Topic:
orders.commands
```

The application needs to publish messages to that topic.

Create a topic-scoped role binding such as:

```text
Principal:
User:mulesoft-order-api-prod

Role:
DeveloperWrite

Resource:
Topic:orders.commands
```

A representative Confluent CLI pattern is:

```bash
confluent iam rbac role-binding create \
  --principal User:<MULESOFT_PRINCIPAL> \
  --role DeveloperWrite \
  --resource Topic:<TOPIC_NAME> \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

Example:

```bash
confluent iam rbac role-binding create \
  --principal User:mulesoft-order-api-prod \
  --role DeveloperWrite \
  --resource Topic:orders.commands \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

The CLI syntax should be validated against the Confluent CLI version installed in the environment.

---

# 20. Consumer Topic Role Binding

Assume MuleSoft consumes from:

```text
orders.events
```

Assign:

```text
Principal:
User:mulesoft-order-api-prod

Role:
DeveloperRead

Resource:
Topic:orders.events
```

Representative command:

```bash
confluent iam rbac role-binding create \
  --principal User:mulesoft-order-api-prod \
  --role DeveloperRead \
  --resource Topic:orders.events \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

---

# 21. Consumer Group Role Binding

A Kafka consumer also uses a consumer group.

For example:

```text
group.id=mulesoft-order-api-prod
```

Create a role binding for that group:

```text
Principal:
User:mulesoft-order-api-prod

Role:
DeveloperRead

Resource:
Group:mulesoft-order-api-prod
```

Representative command:

```bash
confluent iam rbac role-binding create \
  --principal User:mulesoft-order-api-prod \
  --role DeveloperRead \
  --resource Group:mulesoft-order-api-prod \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

This is important because topic authorization and consumer-group authorization represent distinct Kafka resources within Confluent RBAC. Confluent explicitly supports resource-scoped role bindings for both topics and consumer groups. ([Confluent Documentation][1])

---

# 22. Complete Producer Example

Suppose a MuleSoft application has:

```text
Application:
Order API

Principal:
User:mulesoft-order-api-prod

Producer Topic:
orders.commands
```

The authorization model becomes:

```text
User:mulesoft-order-api-prod
              |
              |
       DeveloperWrite
              |
              v
   Topic:orders.commands
```

---

# 23. Complete Consumer Example

Suppose the same application consumes:

```text
Topic:
orders.events

Consumer Group:
mulesoft-order-api-prod
```

The authorization structure becomes:

```text
User:mulesoft-order-api-prod
        |
        +-------------------------+
        |                         |
        v                         v
 DeveloperRead              DeveloperRead
        |                         |
        v                         v
Topic:orders.events    Group:mulesoft-order-api-prod
```

Both bindings should be considered part of the application's Kafka authorization package.

---

# 24. Full MuleSoft RBAC Example

A Mule application might need:

```text
PRINCIPAL
User:mulesoft-order-api-prod
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
    +--> Group:mulesoft-order-api-prod
```

This is preferable to granting broad cluster-level permissions.

---

# 25. Principle of Least Privilege

Role bindings should be as narrowly scoped as practical.

Prefer:

```text
DeveloperWrite
Topic:orders.commands
```

instead of granting a broader role at:

```text
KafkaCluster
```

when the application only requires access to one topic.

Similarly, prefer:

```text
DeveloperRead
Topic:orders.events

DeveloperRead
Group:mulesoft-order-api-prod
```

rather than granting broad read permissions against the entire cluster.

Resource-level role bindings are specifically designed to limit access to defined Confluent resources. ([Confluent Documentation][1])

---

# 26. Environment Separation

Maintain separate principals and bindings for each environment.

For example:

```text
DEV
User:mulesoft-order-api-dev

TEST
User:mulesoft-order-api-test

PROD
User:mulesoft-order-api-prod
```

Then scope the corresponding resources independently:

```text
DEV

User:mulesoft-order-api-dev
    |
    +-- DeveloperWrite → Topic:dev.orders.commands
    +-- DeveloperRead  → Topic:dev.orders.events
    +-- DeveloperRead  → Group:mulesoft-order-api-dev
```

Production:

```text
PROD

User:mulesoft-order-api-prod
    |
    +-- DeveloperWrite → Topic:orders.commands
    +-- DeveloperRead  → Topic:orders.events
    +-- DeveloperRead  → Group:mulesoft-order-api-prod
```

A DEV certificate should never implicitly provide PROD authorization.

---

# 27. Verify Existing Role Bindings

Before troubleshooting application authorization, verify the bindings assigned to the principal.

A representative Confluent CLI query is:

```bash
confluent iam rbac role-binding list \
  --principal User:<MULESOFT_PRINCIPAL> \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

For example:

```bash
confluent iam rbac role-binding list \
  --principal User:mulesoft-order-api-prod \
  --kafka-cluster <KAFKA_CLUSTER_ID>
```

The output should show the expected relationships between:

```text
Principal
Role
Resource
Kafka Cluster
```

---

# 28. Producer Validation

The first functional test should normally be a controlled producer transaction.

Example message:

```json
{
  "source": "mulesoft",
  "type": "connectivity-test",
  "application": "order-api"
}
```

Architecture:

```text
MuleSoft Test Flow
        |
        v
Kafka Producer
        |
        v
orders.commands
```

Validate that:

```text
TLS connection succeeds
mTLS authentication succeeds
Principal is correctly mapped
DeveloperWrite binding is effective
Record reaches Kafka
```

---

# 29. Consumer Validation

Next test consumption.

```text
orders.events
      |
      v
MuleSoft Kafka Consumer
      |
      v
Application Flow
```

Verify:

```text
DeveloperRead → Topic
DeveloperRead → Consumer Group
```

During the initial test, capture:

```text
Topic
Partition
Offset
Key
Timestamp
```

Avoid unnecessarily logging production payloads containing regulated or sensitive data.

---

# 30. Recommended Troubleshooting Order

Troubleshoot the integration in layers.

```text
1. DNS
      ↓
2. Routing
      ↓
3. Firewall
      ↓
4. Kafka TLS endpoint
      ↓
5. Broker certificate
      ↓
6. MuleSoft truststore
      ↓
7. MuleSoft client certificate
      ↓
8. Kafka client certificate trust
      ↓
9. mTLS handshake
      ↓
10. Principal mapping
      ↓
11. Role bindings
      ↓
12. Topic access
      ↓
13. Consumer group access
      ↓
14. Produce/consume test
```

This prevents troubleshooting RBAC when the actual problem exists lower in the connectivity stack.

---

# 31. Troubleshooting: PKIX Path Building Failed

Example:

```text
PKIX path building failed
unable to find valid certification path
```

Likely cause:

```text
MuleSoft does not trust the Kafka broker certificate.
```

Check:

```text
Broker certificate
Intermediate CA
Root CA
MuleSoft truststore
```

Inspect the truststore:

```bash
keytool \
  -list \
  -v \
  -keystore kafka-truststore.p12 \
  -storetype PKCS12
```

---

# 32. Troubleshooting: Certificate Unknown

Example:

```text
SSLHandshakeException
certificate_unknown
```

Possible causes include:

```text
Kafka does not trust the MuleSoft client CA
Incomplete MuleSoft certificate chain
Expired certificate
Wrong certificate presented
Incorrect keystore
```

Inspect:

```bash
openssl x509 \
  -in mulesoft-client.crt \
  -text \
  -noout
```

---

# 33. Troubleshooting: Hostname Verification

Example:

```text
No subject alternative DNS name matching
broker1.kafka.example.com
```

Inspect the Kafka broker certificate.

It should contain an appropriate SAN such as:

```text
Subject Alternative Name:
DNS:broker1.kafka.example.com
```

Do not permanently disable hostname verification to work around an incorrect broker certificate.

---

# 34. Troubleshooting: Authorization Failure

If mTLS succeeds but producing or consuming fails, investigate RBAC.

Check:

```text
Certificate DN
       ↓
Principal mapping
       ↓
Mapped principal
       ↓
Role binding
       ↓
Resource
```

For example:

```text
Expected:

Certificate
CN=mulesoft-order-api-prod

Mapped principal
User:mulesoft-order-api-prod

Binding
DeveloperWrite → Topic:orders.commands
```

A mismatch anywhere in that chain will prevent authorization.

---

# 35. Troubleshooting: Consumer Works Against Topic but Not Group

Verify the consumer group's configuration:

```text
group.id=mulesoft-order-api-prod
```

Then verify that the same principal has:

```text
DeveloperRead
        |
        v
Group:mulesoft-order-api-prod
```

The application may have topic-level read authorization while still lacking authorization for the consumer group.

---

# 36. Troubleshooting: Wrong Principal

A particularly important mTLS/RBAC troubleshooting condition occurs when the certificate is accepted but Confluent derives an unexpected principal.

For example:

```text
EXPECTED

User:mulesoft-order-api-prod
```

but Confluent sees:

```text
User:CN=mulesoft-order-api-prod,OU=Applications,O=Example Corporation
```

The RBAC binding may exist for the first principal but not the second.

The platform team should therefore verify its TLS principal mapping rules. Confluent provides principal mapping specifically for translating certificate DNs into Confluent principals. ([Confluent Documentation][2])

---

# 37. Certificate Rotation

Certificate rotation must preserve the application's Confluent identity whenever possible.

For example, an old certificate may be:

```text
Subject:
CN=mulesoft-order-api-prod
```

and the replacement certificate should normally preserve the same identity:

```text
Subject:
CN=mulesoft-order-api-prod
```

If the principal remains unchanged:

```text
User:mulesoft-order-api-prod
```

existing role bindings can continue to apply.

If the replacement certificate changes the mapped principal, the platform team must create the required role bindings for the new principal before cutover.

Recommended sequence:

```text
Issue replacement certificate
        ↓
Verify principal mapping
        ↓
Confirm role bindings
        ↓
Install new keystore
        ↓
Test non-production
        ↓
Deploy production
        ↓
Validate Kafka operation
        ↓
Retire old certificate
```

---

# 38. MuleSoft Onboarding Request Template

The MuleSoft team should submit:

```text
APPLICATION:
<APPLICATION_NAME>

OWNER:
<APPLICATION_OWNER>

ENVIRONMENT:
<DEV | TEST | PROD>

MULESOFT NETWORK:
<SOURCE_NETWORK>

CERTIFICATE SUBJECT:
<CLIENT_CERTIFICATE_DN>

PRODUCER TOPICS:
<TOPICS>

CONSUMER TOPICS:
<TOPICS>

CONSUMER GROUPS:
<GROUPS>

EXPECTED MESSAGE RATE:
<MESSAGES_PER_SECOND>

MAXIMUM MESSAGE SIZE:
<SIZE>

DATA CLASSIFICATION:
<CLASSIFICATION>
```

The MuleSoft team does not necessarily need to determine the final Confluent principal itself. That should be validated by the platform team against the configured principal mapping rules.

---

# 39. Confluent Platform Team Response

The Confluent platform team should return:

```text
KAFKA ENVIRONMENT:
<ENVIRONMENT>

KAFKA CLUSTER ID:
<CLUSTER_ID>

BOOTSTRAP SERVERS:
<BROKER1:PORT,BROKER2:PORT,BROKER3:PORT>

AUTHENTICATION:
mTLS

SERVER CA:
<CA_INFORMATION>

CLIENT CERTIFICATE DN:
<CERTIFICATE_DN>

MAPPED PRINCIPAL:
User:<PRINCIPAL>

ROLE BINDINGS:

DeveloperWrite
Topic:<PRODUCER_TOPIC>

DeveloperRead
Topic:<CONSUMER_TOPIC>

DeveloperRead
Group:<CONSUMER_GROUP>
```

---

# 40. Onboarding Responsibility Matrix

| Activity                       | MuleSoft Team | Confluent Team | Network Team | PKI Team |
| ------------------------------ | ------------: | -------------: | -----------: | -------: |
| Mule application configuration |             X |                |              |          |
| Kafka Connector configuration  |             X |                |              |          |
| Client keystore installation   |             X |                |              |          |
| Kafka topics                   |               |              X |              |          |
| Consumer group definition      |             X |              X |              |          |
| Principal mapping              |               |              X |              |          |
| Confluent role bindings        |               |              X |              |          |
| Client certificate request     |             X |                |              |        X |
| Certificate issuance           |               |                |              |        X |
| Broker certificates            |               |              X |              |        X |
| DNS                            |               |                |            X |          |
| Firewall                       |               |                |            X |          |
| Routing                        |               |                |            X |          |
| End-to-end testing             |             X |              X |            X |          |

---

# 41. Production Readiness Checklist

* [ ] MuleSoft can resolve all required Kafka broker names.
* [ ] MuleSoft can reach all Kafka broker listener ports.
* [ ] Kafka broker certificates are valid.
* [ ] Broker SAN entries match advertised hostnames.
* [ ] MuleSoft trusts the Kafka issuing CA.
* [ ] MuleSoft keystore contains the correct client certificate.
* [ ] MuleSoft keystore contains the associated private key.
* [ ] Client certificate is within its validity period.
* [ ] Kafka trusts the MuleSoft client certificate CA.
* [ ] mTLS handshake succeeds.
* [ ] Certificate DN is correctly mapped to the expected Confluent principal.
* [ ] Principal has `DeveloperWrite` against required producer topics.
* [ ] Principal has `DeveloperRead` against required consumer topics.
* [ ] Principal has `DeveloperRead` against required consumer groups.
* [ ] No unnecessary cluster-wide role bindings have been assigned.
* [ ] Producer test succeeds.
* [ ] Consumer test succeeds.
* [ ] Certificate expiration monitoring is configured.
* [ ] Application and platform ownership are documented.
* [ ] Secrets are stored through an approved secrets-management mechanism.

---

# 42. Recommended Enterprise Onboarding Workflow

```text
MuleSoft Team
      |
      | Submit onboarding request
      v
Confluent Platform Team
      |
      | Determine required resources
      v
PKI Team
      |
      | Issue MuleSoft certificate
      v
Confluent Platform Team
      |
      | Verify certificate DN
      | Verify principal mapping
      v
Network Team
      |
      | Configure DNS/routing/firewall
      v
Confluent Platform Team
      |
      | Bind DeveloperWrite to producer topics
      | Bind DeveloperRead to consumer topics
      | Bind DeveloperRead to consumer groups
      v
MuleSoft Team
      |
      | Configure truststore
      | Configure keystore
      | Configure Kafka Connector
      v
Integration Testing
      |
      | Producer validation
      | Consumer validation
      v
Production Approval
```

The central security relationship should always be easy to trace:

```text
X.509 CERTIFICATE
        |
        v
CERTIFICATE DN
        |
        v
PRINCIPAL MAPPING
        |
        v
CONFLUENT PRINCIPAL
        |
        v
ROLE BINDING
        |
        +------------------+
        |                  |
        v                  v
      TOPIC          CONSUMER GROUP
```

For MuleSoft administrators, the most important distinction is that **mTLS establishes identity, while Confluent RBAC determines resource access**. A successful TLS handshake confirms that the client has authenticated; it does not by itself grant permission to produce to a topic or consume through a consumer group. Those permissions should be provided through resource-scoped Confluent role bindings mapped to the principal derived from the MuleSoft certificate. ([Confluent Documentation][4])

[1]: https://docs.confluent.io/platform/current/security/authorization/rbac/rbac-predefined-roles.html?utm_source=chatgpt.com "Use Predefined RBAC Roles in Confluent Platform"
[2]: https://docs.confluent.io/platform/current/security/authentication/mutual-tls/tls-principal-mapping.html?utm_source=chatgpt.com "Use Principal Mapping in Confluent Platform"
[3]: https://docs.mulesoft.com/mule-runtime/latest/tls-configuration?utm_source=chatgpt.com "Configure TLS with Keystores and Truststores"
[4]: https://docs.confluent.io/platform/current/kafka/configure-mds/mutual-tls-auth-rbac.html?utm_source=chatgpt.com "Configure mTLS authentication and RBAC for Kafka brokers"
