
# Confluent Platform on OpenShift

## Detailed Platform Engineering and Operations Learning Plan

## 1. Purpose

This learning plan prepares engineers to build, secure, operate, troubleshoot, and recover an enterprise Confluent Platform deployment with the following characteristics:

* Confluent Platform deployed on-premises.
* Red Hat OpenShift Container Platform as the orchestration platform.
* Confluent for Kubernetes, or CFK, as the Confluent operator.
* KRaft-based Kafka clusters.
* Separate development, test, and production environments.
* Multiple application teams and tenants.
* Mutual TLS, or mTLS, authentication.
* OAuth 2.0 and OpenID Connect integration.
* Confluent RBAC and Kafka ACL authorization.
* Kafka Connect source and sink connectors.
* Schema Registry and formal data-contract management.
* East-west multi-data-center resiliency and failover.
* Production monitoring, upgrades, incident response, and disaster recovery.

CFK provides a declarative control plane for deploying and managing Confluent Platform in Kubernetes private-cloud environments. It supports operator-based reconciliation, scaling, configuration changes, security integration, and component lifecycle management. ([Confluent Documentation][1])

---

# 2. Target Outcomes

By completing this plan, an engineer should be able to:

1. Explain Kafka brokers, controllers, topics, partitions, replicas, consumer groups, and offsets.
2. Build a KRaft-based Confluent Platform cluster on OpenShift.
3. Configure persistent storage, scheduling, networking, and failure-domain placement.
4. Deploy and manage Confluent components through CFK custom resources.
5. Implement TLS and mTLS for interbroker, component, and client communication.
6. Integrate Kafka and Confluent services with an OIDC identity provider.
7. Implement authorization through Confluent RBAC and Kafka ACLs.
8. Design and support a multi-tenant platform.
9. Deploy, update, monitor, and troubleshoot Kafka connectors.
10. Operate Schema Registry and enforce compatibility rules.
11. Monitor multiple development, test, and production clusters.
12. Execute east-west failover and failback.
13. Perform rolling upgrades and certificate rotations.
14. Diagnose incidents across Kafka, CFK, OpenShift, storage, networking, security, and client applications.

---

# 3. Recommended Training Schedule

| Phase | Subject                                | Recommended Duration |
| ----- | -------------------------------------- | -------------------: |
| 1     | Kafka fundamentals                     |              2 weeks |
| 2     | OpenShift fundamentals                 |              2 weeks |
| 3     | OpenShift stateful workload operations |              2 weeks |
| 4     | CFK and Confluent deployment           |              3 weeks |
| 5     | TLS, mTLS, OIDC, RBAC, and ACLs        |              3 weeks |
| 6     | Multi-tenancy                          |               1 week |
| 7     | Kafka Connect                          |              2 weeks |
| 8     | Schema Registry                        |               1 week |
| 9     | Monitoring and troubleshooting         |              2 weeks |
| 10    | Multi-data-center failover             |              3 weeks |
| 11    | Upgrades and operational readiness     |              2 weeks |

**Total recommended duration:** 20–23 weeks at approximately eight hours per week.

The program can be compressed into eight to ten weeks when completed as a full-time lab curriculum.

---

# 4. Required Lab Environment

The student should have access to a nonproduction OpenShift environment.

## Minimum recommended lab

* One OpenShift cluster.
* Three OpenShift worker nodes.
* One dedicated project or namespace.
* Dynamic persistent-volume provisioning.
* Helm 3.
* OpenShift CLI, `oc`.
* Java 17 or the Java version supported by the selected Confluent release.
* `curl`, `openssl`, `jq`, `keytool`, and Git.
* Access to Confluent Platform container images.
* A CFK-compatible Confluent Platform version.
* A test OIDC provider such as Keycloak or the enterprise development IdP.
* One test PostgreSQL database.
* Optional Oracle and DB2 development databases.
* Prometheus and Grafana or the OpenShift monitoring stack.

## Installation references

* [Install the OpenShift CLI](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/cli_tools/openshift-cli-oc)
* [Helm documentation](https://helm.sh/docs/)
* [Deploy Confluent for Kubernetes](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
* [Plan a CFK deployment](https://docs.confluent.io/operator/current/co-plan.html)
* [CFK overview](https://docs.confluent.io/operator/current/overview.html)
* [Confluent for Kubernetes examples repository](https://github.com/confluentinc/confluent-kubernetes-examples)

Before beginning the lab, verify the supported combinations of OpenShift, Kubernetes, CFK, and Confluent Platform versions in the current CFK support documentation. Confluent recommends planning storage, security, networking, sizing, image access, and upgrade strategy before deployment. ([Confluent Documentation][2])

---

# 5. Phase 1: Kafka Fundamentals

## Objective

Understand Kafka as a distributed system before introducing OpenShift and CFK.

## Topics

* Brokers.
* KRaft controllers.
* Metadata quorum.
* Topics.
* Partitions.
* Partition leaders.
* Follower replicas.
* In-sync replicas.
* Replication factor.
* Minimum in-sync replicas.
* Producer acknowledgements.
* Consumer groups.
* Consumer offsets.
* Rebalancing.
* Retention.
* Log compaction.
* Idempotent production.
* Transactions.
* Exactly-once processing.
* Rack awareness.
* Leader election.
* Under-replicated partitions.
* Offline partitions.

## Required learning material

### Introductory courses

1. [Apache Kafka 101](https://developer.confluent.io/courses/apache-kafka/)
2. [Kafka Architecture](https://developer.confluent.io/courses/architecture/get-started/)
3. [Kafka Topics](https://developer.confluent.io/courses/apache-kafka/topics/)
4. [Kafka Producers](https://developer.confluent.io/courses/apache-kafka/producer-application/)
5. [Kafka Consumers](https://developer.confluent.io/courses/apache-kafka/consumer-application/)
6. [Kafka Consumer Groups and Rebalancing](https://developer.confluent.io/courses/architecture/consumer-group-protocol/)
7. [Kafka Transactions](https://developer.confluent.io/courses/architecture/transactions/)

### Administrator references

* [Kafka command-line tools](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)
* [Kafka topic operations](https://docs.confluent.io/kafka/operations-tools/topic-operations.html)
* [Manage Kafka consumer groups](https://docs.confluent.io/kafka/operations-tools/manage-consumer-groups.html)
* [Kafka broker configuration reference](https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html)
* [Kafka producer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
* [Kafka consumer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)

Kafka administrators routinely use `kafka-topics`, `kafka-configs`, and `kafka-consumer-groups` to inspect and modify resources. The official topic-operations guide provides create, describe, alter, and delete procedures, while the consumer-group guide covers group membership, offsets, and lag. ([Confluent Documentation][3])

## Lab 1: Topic administration

Follow:

* [Kafka topic operations](https://docs.confluent.io/kafka/operations-tools/topic-operations.html)

Complete the following:

```bash
kafka-topics \
  --bootstrap-server <bootstrap-server> \
  --create \
  --topic training.orders \
  --partitions 6 \
  --replication-factor 3
```

```bash
kafka-topics \
  --bootstrap-server <bootstrap-server> \
  --describe \
  --topic training.orders
```

```bash
kafka-configs \
  --bootstrap-server <bootstrap-server> \
  --entity-type topics \
  --entity-name training.orders \
  --alter \
  --add-config retention.ms=86400000
```

The student must explain:

* Which broker leads each partition.
* Which brokers hold replicas.
* Whether every replica is in the ISR.
* What happens when one broker becomes unavailable.
* Why increasing partitions can affect ordering.

## Lab 2: Producers and consumers

Follow:

* [Kafka console producer and consumer tools](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)
* [Kafka producer documentation](https://docs.confluent.io/platform/current/clients/producer.html)
* [Kafka consumer documentation](https://docs.confluent.io/platform/current/clients/consumer.html)

Complete the following:

1. Produce messages with keys.
2. Consume from the beginning.
3. Run two consumers with the same group ID.
4. Add and remove consumers.
5. Observe partition assignment and rebalancing.
6. Stop a consumer without a graceful shutdown.
7. Compare committed offsets with log-end offsets.

## Lab 3: Consumer lag

Follow:

* [Manage Kafka consumer groups](https://docs.confluent.io/kafka/operations-tools/manage-consumer-groups.html)

```bash
kafka-consumer-groups \
  --bootstrap-server <bootstrap-server> \
  --group training-consumer \
  --describe
```

The student must identify:

* Current offset.
* Log-end offset.
* Lag.
* Consumer instance.
* Host.
* Partition assignment.

## Phase completion test

The student must explain:

* The relationship between replication factor and `min.insync.replicas`.
* The behavior of `acks=all`.
* The difference between a broker and a controller.
* Why adding brokers does not automatically rebalance existing data.
* Why adding topic partitions can affect key placement.
* How consumer lag differs from throughput.

---

# 6. Phase 2: OpenShift Fundamentals

## Objective

Develop the OpenShift skills required to operate stateful Confluent workloads.

## Required topics

* Projects and namespaces.
* Pods.
* StatefulSets.
* Deployments.
* Services.
* Routes and ingress.
* ConfigMaps.
* Secrets.
* Service accounts.
* Roles and role bindings.
* Security Context Constraints.
* PersistentVolumes.
* PersistentVolumeClaims.
* StorageClasses.
* Operators.
* Custom resources.
* Node selectors.
* Taints and tolerations.
* Pod affinity and anti-affinity.
* Resource requests and limits.
* Pod disruption budgets.
* NetworkPolicies.
* Cluster DNS.
* OpenShift monitoring.

## Required learning material

### Red Hat courses and labs

* [Red Hat OpenShift learning resources](https://developers.redhat.com/learn/openshift)
* [OpenShift interactive learning scenarios](https://developers.redhat.com/learn)
* [OpenShift Container Platform documentation](https://docs.redhat.com/en/documentation/openshift_container_platform/)
* [OpenShift CLI documentation](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/cli_tools/openshift-cli-oc)
* [OpenShift architecture](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/architecture/)
* [Managing workloads](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/building_applications/)
* [Authentication and authorization](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/authentication_and_authorization/)
* [Networking](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/networking/)
* [Storage](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/storage/)
* [Operators](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/operators/)

The OpenShift documentation provides separate administration guides for networking, storage, authentication, operators, workload management, scalability, and troubleshooting. ([Red Hat Documentation][4])

## Lab 1: Project and access setup

Create a project:

```bash
oc new-project confluent-training
```

Review access:

```bash
oc auth can-i --list -n confluent-training
oc get rolebindings -n confluent-training
oc get serviceaccounts -n confluent-training
```

Follow:

* [Using RBAC to define and apply permissions](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/authentication_and_authorization/using-rbac)
* [Managing service accounts](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/authentication_and_authorization/understanding-and-creating-service-accounts)

## Lab 2: Stateful workload

Follow:

* [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
* [OpenShift persistent storage](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/storage/understanding-persistent-storage)

Complete the following:

1. Create a PVC.
2. Deploy a StatefulSet that writes data to the PVC.
3. Delete the pod.
4. Verify the replacement pod mounts the original volume.
5. Scale the StatefulSet.
6. Identify the PVC assigned to each pod.
7. Drain the node and observe volume attachment behavior.

## Lab 3: Resource management

Follow:

* [Managing resource quotas](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/building_applications/quotas)
* [Managing limits and requests](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/nodes/working-with-clusters)

The student must configure:

* CPU requests and limits.
* Memory requests and limits.
* Namespace quotas.
* LimitRanges.
* Pod anti-affinity.
* A PodDisruptionBudget.

## Lab 4: OpenShift troubleshooting

Use:

```bash
oc get pods -n confluent-training -o wide
oc describe pod <pod> -n confluent-training
oc logs <pod> -n confluent-training
oc logs <pod> -c <container> -n confluent-training
oc get events -n confluent-training --sort-by=.lastTimestamp
oc get pvc,pv -n confluent-training
oc get svc,endpoints,route -n confluent-training
oc adm top pods -n confluent-training
oc adm top nodes
```

Follow:

* [OpenShift troubleshooting](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/support/troubleshooting)
* [Gathering cluster data](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/support/gathering-cluster-data)
* [Investigating pod issues](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/support/troubleshooting)

The OpenShift `must-gather` process collects cluster resource definitions and service logs that are commonly required for incident analysis and support escalation. ([Red Hat Documentation][5])

```bash
oc adm must-gather
```

## Phase completion test

The student must diagnose deliberately introduced examples of:

* `Pending`.
* `CrashLoopBackOff`.
* `ImagePullBackOff`.
* Failed readiness probes.
* PVC binding failures.
* Insufficient CPU.
* Insufficient memory.
* NetworkPolicy rejection.
* DNS resolution failure.
* Service selector mismatch.

---

# 7. Phase 3: OpenShift Networking and Storage

## Objective

Understand the two OpenShift subsystems most likely to affect Kafka availability and performance.

## Networking learning material

* [OpenShift networking overview](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/networking/networking-overview)
* [OVN-Kubernetes network plugin](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/ovn-kubernetes_network_plugin/)
* [NetworkPolicy configuration](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/network_security/network-policy)
* [Ingress and load balancing](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/ingress_and_load_balancing/)
* [DNS configuration](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/networking_operators/dns-operator)
* [Troubleshooting OVN-Kubernetes](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/ovn-kubernetes_network_plugin/ovn-kubernetes-troubleshooting-sources)

## Networking lab

The student must:

1. Resolve Kafka and Schema Registry service names from another pod.
2. Connect to Kafka service ports using `nc` or `openssl`.
3. Create a NetworkPolicy that blocks Kafka.
4. Identify the policy as the cause.
5. Permit only an approved namespace.
6. Verify ingress and egress paths.
7. Record east-west data-center latency.
8. Test packet size and MTU assumptions.
9. Verify all required firewall rules.
10. Document internal and external bootstrap addresses.

Useful commands:

```bash
oc exec -it <diagnostic-pod> -- nslookup <service-name>
oc exec -it <diagnostic-pod> -- nc -vz <service-name> <port>
oc exec -it <diagnostic-pod> -- curl -vk https://<service>:<port>
oc exec -it <diagnostic-pod> -- openssl s_client \
  -connect <service>:<port> \
  -servername <dns-name> \
  -showcerts
```

## Storage learning material

* [OpenShift storage overview](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/storage/understanding-persistent-storage)
* [Persistent-volume claims](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/storage/understanding-persistent-storage)
* [Dynamic provisioning](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/storage/dynamic-provisioning)
* [Expanding persistent volumes](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/storage/expanding-persistent-volumes)
* [OpenShift Data Foundation troubleshooting](https://docs.redhat.com/en/documentation/red_hat_openshift_data_foundation/4.18/html/troubleshooting_openshift_data_foundation/)

## Storage lab

The student must:

1. Identify the Kafka StorageClass.
2. Determine whether it provides block or file storage.
3. Record expected IOPS and throughput.
4. Expand a PVC in the test environment.
5. Fill a volume to an alert threshold.
6. Identify storage pressure before Kafka becomes unavailable.
7. Replace a failed stateful pod without deleting the PVC.
8. Verify node and volume topology constraints.

---

# 8. Phase 4: Install and Operate Confluent for Kubernetes

## Objective

Install CFK and deploy a complete KRaft-based Confluent environment.

## Required learning material

* [CFK overview](https://docs.confluent.io/operator/current/overview.html)
* [Plan a CFK deployment](https://docs.confluent.io/operator/current/co-plan.html)
* [Deploy CFK](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
* [CFK quick start](https://docs.confluent.io/operator/current/co-quickstart.html)
* [CFK custom resource definitions](https://docs.confluent.io/operator/current/co-api.html)
* [Deploy Confluent Platform components](https://docs.confluent.io/operator/current/co-deploy-cp.html)
* [Configure Confluent Platform with CFK](https://docs.confluent.io/operator/current/co-configure.html)
* [CFK examples](https://github.com/confluentinc/confluent-kubernetes-examples)

CFK can be installed through Confluent’s Helm repository, from the downloaded CFK bundle, or through OpenShift OperatorHub when that installation method is supported. ([Confluent Documentation][6])

## Lab 1: Install CFK

Follow:

* [Deploy CFK directly from the Helm repository](https://docs.confluent.io/operator/current/co-deploy-cfk.html)

Typical workflow:

```bash
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
```

```bash
helm upgrade --install confluent-operator \
  confluentinc/confluent-for-kubernetes \
  --namespace confluent-training \
  --set namespaced=true
```

Verify:

```bash
oc get deployments -n confluent-training
oc get pods -n confluent-training
oc get crds | grep platform.confluent.io
oc logs deployment/confluent-operator -n confluent-training
```

The student must document:

* Operator namespace.
* Namespaced or cluster-wide scope.
* Operator service account.
* Roles and bindings.
* CRDs installed.
* Operator image version.
* Supported Confluent Platform version.

## Lab 2: Deploy KRaft controllers and Kafka

Follow:

* [Deploy KRaft controllers using CFK](https://docs.confluent.io/operator/current/co-deploy-kraft.html)
* [Deploy Kafka using CFK](https://docs.confluent.io/operator/current/co-deploy-cp.html)
* [Configure storage](https://docs.confluent.io/operator/current/co-storage.html)
* [Configure pod scheduling](https://docs.confluent.io/operator/current/co-pod-scheduling.html)

The deployment must include:

* Three KRaft controllers.
* Three Kafka brokers.
* Persistent storage.
* CPU and memory requests.
* Pod anti-affinity.
* Rack awareness.
* Pod disruption budgets.
* Internal listeners.
* TLS-ready listener definitions.

Verify:

```bash
oc get kraftcontroller,kafka -n confluent-training
oc describe kafka kafka -n confluent-training
oc get pods -n confluent-training -o wide
oc get pvc -n confluent-training
oc get events -n confluent-training --sort-by=.lastTimestamp
```

## Lab 3: Deploy supporting components

Deploy:

* Schema Registry.
* Kafka Connect.
* Control Center, when licensed and used.
* REST Proxy, when used.
* ksqlDB, when used.

References:

* [Deploy Schema Registry with CFK](https://docs.confluent.io/operator/current/co-deploy-cp.html)
* [Deploy Kafka Connect with CFK](https://docs.confluent.io/operator/current/co-deploy-cp.html)
* [Deploy Control Center with CFK](https://docs.confluent.io/operator/current/co-deploy-cp.html)
* [Component custom-resource API](https://docs.confluent.io/operator/current/co-api.html)

## Lab 4: CFK reconciliation

Complete the following:

1. Change a non-restart configuration.
2. Observe CFK reconciliation.
3. Change a configuration that requires a rolling restart.
4. Observe pod replacement order.
5. Introduce an invalid custom-resource value.
6. Review resource conditions and operator logs.
7. Correct the configuration.
8. Verify that CFK returns the component to the desired state.

References:

* [CFK configuration](https://docs.confluent.io/operator/current/co-configure.html)
* [CFK troubleshooting](https://docs.confluent.io/operator/current/co-troubleshooting.html)
* [CFK resource status](https://docs.confluent.io/operator/current/co-api.html)

## Phase completion test

The student must identify whether a failed deployment is caused by:

* Invalid CFK syntax.
* Missing secret.
* Unsupported configuration.
* PVC failure.
* Security Context Constraint.
* Insufficient node resources.
* Image registry access.
* Operator permissions.
* Failed readiness.
* Kafka process failure.

---

# 9. Phase 5: TLS and mTLS

## Objective

Secure communications and implement certificate-based client authentication.

## Required learning material

### TLS fundamentals

* [OpenSSL documentation](https://docs.openssl.org/)
* [Java `keytool` documentation](https://docs.oracle.com/en/java/javase/17/docs/specs/man/keytool.html)
* [Confluent security overview](https://docs.confluent.io/platform/current/security/overview.html)
* [TLS encryption in Confluent Platform](https://docs.confluent.io/platform/current/security/encryption.html)
* [mTLS authentication overview](https://docs.confluent.io/platform/current/security/authentication/mutual-tls/overview.html)
* [TLS principal mapping](https://docs.confluent.io/platform/current/security/authentication/mutual-tls/tls-principal-mapping.html)
* [Configure mTLS and RBAC for brokers](https://docs.confluent.io/platform/current/kafka/configure-mds/mutual-tls-auth-rbac.html)
* [Configure TLS with CFK](https://docs.confluent.io/operator/current/co-network-encryption.html)
* [Provide certificates to CFK](https://docs.confluent.io/operator/current/co-network-encryption.html)

Confluent supports TLS encryption and mTLS client authentication. In an mTLS connection, both the server and client present certificates, and certificate distinguished names can be mapped into application principals. ([Confluent Documentation][7])

## Lab 1: Create a lab certificate authority

Create:

* Root CA.
* Optional intermediate CA.
* Broker certificate.
* Controller certificate.
* Schema Registry certificate.
* Connect certificate.
* Client certificate.

Example root CA:

```bash
openssl genrsa -out root-ca.key 4096
```

```bash
openssl req -x509 -new -nodes \
  -key root-ca.key \
  -sha256 \
  -days 3650 \
  -out root-ca.crt
```

The student must understand that production certificates should be issued through the organization’s approved PKI process rather than a lab CA.

## Lab 2: Validate certificate content

```bash
openssl x509 \
  -in broker.crt \
  -text \
  -noout
```

Confirm:

* Issuer.
* Subject.
* Expiration date.
* Subject alternative names.
* Key usage.
* Extended key usage.
* Certificate chain.

## Lab 3: Configure TLS through CFK

Follow:

* [Configure network encryption with CFK](https://docs.confluent.io/operator/current/co-network-encryption.html)

The student must:

1. Create OpenShift TLS secrets.
2. Reference the secrets from CFK resources.
3. Configure broker and component listeners.
4. Verify TLS negotiation.
5. Configure a client truststore.
6. Connect using a Kafka CLI client.

## Lab 4: Configure mTLS

Follow:

* [mTLS authentication overview](https://docs.confluent.io/platform/current/security/authentication/mutual-tls/overview.html)
* [Configure mTLS and RBAC for Kafka brokers](https://docs.confluent.io/platform/current/kafka/configure-mds/mutual-tls-auth-rbac.html)
* [TLS principal mapping](https://docs.confluent.io/platform/current/security/authentication/mutual-tls/tls-principal-mapping.html)

Client properties:

```properties
security.protocol=SSL
ssl.truststore.location=/path/client.truststore.p12
ssl.truststore.password=<password>
ssl.keystore.location=/path/client.keystore.p12
ssl.keystore.password=<password>
ssl.key.password=<password>
```

Test:

```bash
kafka-topics \
  --bootstrap-server <bootstrap-server> \
  --command-config client.properties \
  --list
```

## Lab 5: TLS failure diagnosis

Introduce and diagnose:

* Expired client certificate.
* Incorrect SAN.
* Untrusted issuer.
* Missing intermediate certificate.
* Invalid private key.
* Certificate and key mismatch.
* Incorrect truststore password.
* Incorrect keystore password.
* Principal-mapping failure.

Use:

```bash
openssl s_client \
  -connect <broker-host>:<port> \
  -servername <broker-dns-name> \
  -CAfile root-ca.crt \
  -cert client.crt \
  -key client.key \
  -showcerts
```

## Lab 6: Certificate rotation

Follow:

* [Rotate TLS certificates with CFK](https://docs.confluent.io/operator/current/co-network-encryption.html)

The student must:

1. Add trust for the new CA or intermediate.
2. Verify both old and new chains are trusted.
3. Replace server certificates.
4. Replace client certificates.
5. Remove the old trust only after all components have migrated.
6. Verify no production outage.
7. Document rollback steps.

---

# 10. Phase 6: OAuth and OIDC Integration

## Objective

Integrate Confluent Platform with an enterprise identity provider for users and workloads.

## Required concepts

* OAuth 2.0.
* OpenID Connect.
* Issuer.
* Discovery endpoint.
* Authorization endpoint.
* Token endpoint.
* JWKS endpoint.
* Access token.
* ID token.
* JWT claims.
* Audience.
* Scope.
* Subject.
* Groups.
* Client ID.
* Client secret.
* Authorization code flow.
* Client-credentials flow.
* Token expiration.
* Clock skew.
* Workload identities.
* Human identities.

## Required learning material

* [OAuth/OIDC authentication overview](https://docs.confluent.io/platform/current/security/authentication/oauth-oidc/overview.html)
* [SASL/OAUTHBEARER authentication](https://docs.confluent.io/platform/current/security/authentication/sasl/oauthbearer/overview.html)
* [OAuth/OIDC and RBAC client flow](https://docs.confluent.io/platform/current/security/authorization/rbac/client-flow-oauth-oidc-and-rbac.html)
* [Configure OAuth/OIDC with CFK](https://docs.confluent.io/operator/current/co-authenticate-oauth.html)
* [OpenID Connect Core specification](https://openid.net/specs/openid-connect-core-1_0.html)
* [OAuth 2.0 authorization framework](https://datatracker.ietf.org/doc/html/rfc6749)
* [JSON Web Token specification](https://datatracker.ietf.org/doc/html/rfc7519)

Confluent Platform supports OAuth and OIDC authentication for users and workloads. Separate identity providers may be used to distinguish human identities from workload identities. ([Confluent Documentation][8])

## Lab 1: Inspect the identity provider

Retrieve:

```bash
curl -s \
  https://<idp-host>/<issuer-path>/.well-known/openid-configuration \
  | jq
```

Identify:

* Issuer.
* Token endpoint.
* Authorization endpoint.
* JWKS URI.
* Supported scopes.
* Supported grant types.

## Lab 2: Obtain a workload token

```bash
curl -s \
  -X POST https://<idp-host>/<token-endpoint> \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=<client-id>" \
  -d "client_secret=<client-secret>" \
  -d "scope=<scope>"
```

The student must inspect:

* `iss`.
* `sub`.
* `aud`.
* `exp`.
* `iat`.
* Group or role claims.

## Lab 3: Configure OIDC with CFK

Follow:

* [Configure OAuth authentication with CFK](https://docs.confluent.io/operator/current/co-authenticate-oauth.html)
* [SASL/OAUTHBEARER configuration](https://docs.confluent.io/platform/current/security/authentication/sasl/oauthbearer/overview.html)

Configure:

* OIDC issuer.
* JWKS endpoint.
* Expected audience.
* Principal claim.
* Group claim where used.
* Client credentials secret.
* Broker listener.
* Confluent component authentication.
* Client authentication.

## Lab 4: Diagnose token failures

Introduce:

* Wrong issuer.
* Wrong audience.
* Expired token.
* Missing scope.
* Missing group claim.
* Invalid signature.
* Unreachable JWKS endpoint.
* Identity-provider certificate failure.
* Excessive clock skew.

The student must distinguish:

* Token acquisition failure.
* Token validation failure.
* Authentication success followed by authorization failure.

---

# 11. Phase 7: RBAC and Kafka ACLs

## Objective

Implement least-privilege authorization for users, applications, and administrators.

## Learning material

### Kafka ACLs

* [Kafka ACL overview](https://docs.confluent.io/platform/current/security/authorization/acls/overview.html)
* [Manage ACLs with `kafka-acls`](https://docs.confluent.io/platform/current/security/authorization/acls/manage-acls.html)
* [Kafka ACL command reference](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)

### Confluent RBAC

* [Confluent RBAC overview](https://docs.confluent.io/platform/current/security/authorization/rbac/overview.html)
* [RBAC predefined roles](https://docs.confluent.io/platform/current/security/authorization/rbac/predefined-rbac-roles.html)
* [Configure Metadata Service](https://docs.confluent.io/platform/current/kafka/configure-mds/index.html)
* [Create role bindings](https://docs.confluent.io/platform/current/security/authorization/rbac/manage-role-bindings.html)
* [mTLS and RBAC](https://docs.confluent.io/platform/current/kafka/configure-mds/mutual-tls-auth-rbac.html)
* [OAuth/OIDC and RBAC](https://docs.confluent.io/platform/current/security/authorization/rbac/client-flow-oauth-oidc-and-rbac.html)

Kafka ACLs control which principals can perform operations on topics, consumer groups, clusters, and other Kafka resources. KRaft-based clusters can use Kafka’s `StandardAuthorizer`. ([Confluent Documentation][9])

## Lab 1: Producer and consumer ACLs

Create a service principal that can:

* Write to one topic.
* Describe that topic.
* Read from the topic.
* Use one consumer-group prefix.

Verify that it cannot:

* Access another tenant’s topic.
* Use another tenant’s consumer group.
* Create topics.
* Alter broker configuration.
* Read cluster metadata beyond required operations.

## Lab 2: Confluent RBAC

Create role bindings for:

* System administrator.
* Cluster administrator.
* Developer.
* Resource owner.
* Security administrator.
* Read-only operations user.
* Connector service identity.
* Schema administrator.

The student must explain:

* Scope.
* Resource patterns.
* Cluster-level privileges.
* Topic-level privileges.
* Schema Registry privileges.
* Connect privileges.
* Separation of duties.

## Lab 3: Authorization troubleshooting

Use:

```bash
kafka-acls \
  --bootstrap-server <bootstrap-server> \
  --command-config admin.properties \
  --list
```

Diagnose:

* `TopicAuthorizationException`.
* `GroupAuthorizationException`.
* `ClusterAuthorizationException`.
* Schema Registry HTTP 403.
* Connect HTTP 403.
* Authenticated user with no applicable role binding.

---

# 12. Phase 8: Multi-Tenant Design

## Objective

Create a controlled shared platform for independent application teams.

## Tenant-isolation areas

* Service identities.
* Certificates and OAuth clients.
* Topics.
* Consumer groups.
* Transactional IDs.
* Schema subjects.
* Connect workers.
* Connector credentials.
* Namespace resources.
* Network access.
* Storage and retention.
* Resource quotas.
* Producer and consumer quotas.
* Administrative ownership.
* Monitoring and alert routing.

## Required learning material

* [Kafka quotas](https://docs.confluent.io/platform/current/kafka/design.html#quotas)
* [Kafka dynamic configuration](https://docs.confluent.io/platform/current/kafka/dynamic-config.html)
* [Kafka ACLs](https://docs.confluent.io/platform/current/security/authorization/acls/overview.html)
* [Confluent RBAC predefined roles](https://docs.confluent.io/platform/current/security/authorization/rbac/predefined-rbac-roles.html)
* [Schema Registry contexts](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-registry-multi-tenancy.html)
* [OpenShift resource quotas](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/building_applications/quotas)
* [OpenShift NetworkPolicy](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/network_security/network-policy)

## Recommended hierarchy

```text
Environment
└── Business domain
    └── Tenant
        ├── Service identities
        ├── Topics
        ├── Consumer groups
        ├── Transactional IDs
        ├── Schemas
        ├── Connectors
        ├── Quotas
        └── Monitoring ownership
```

## Tenant onboarding lab

Create two tenants:

```text
finance
human-resources
```

The student must prove:

* Finance cannot consume HR topics.
* HR cannot register finance schemas.
* Finance cannot use HR consumer-group IDs.
* HR connectors cannot read finance secrets.
* Each tenant uses separate credentials.
* Each tenant has an owner and support contact.
* Each tenant has documented topic and schema naming conventions.

## Tenant onboarding checklist

For every tenant, create:

1. Business owner.
2. Technical owner.
3. Data classification.
4. Service identity.
5. Authentication method.
6. Authorization bindings.
7. Topic prefix.
8. Consumer-group prefix.
9. Schema subject strategy.
10. Connect deployment or worker group.
11. Quota.
12. Retention standard.
13. Monitoring dashboard.
14. Alert routing.
15. Support runbook.
16. Decommissioning process.

---

# 13. Phase 9: Kafka Connect

## Objective

Deploy and troubleshoot source and sink connectors in distributed mode.

## Required concepts

* Connect workers.
* Worker groups.
* Distributed mode.
* Connect internal topics.
* Connectors.
* Tasks.
* Offsets.
* Converters.
* Serializers.
* Transformations.
* Plugin path.
* Connector REST API.
* Error tolerance.
* Retries.
* Dead-letter queues.
* Database credentials.
* Source-system privileges.
* Sink-system idempotency.

## Required learning material

* [Kafka Connect overview](https://docs.confluent.io/platform/current/connect/index.html)
* [Kafka Connect course](https://developer.confluent.io/courses/kafka-connect/intro/)
* [Kafka Connect user guide](https://docs.confluent.io/platform/current/connect/userguide.html)
* [Configure self-managed connectors](https://docs.confluent.io/platform/current/connect/configuring.html)
* [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)
* [Connect worker configuration](https://docs.confluent.io/platform/current/connect/references/allconfigs.html)
* [Connector catalog](https://docs.confluent.io/platform/current/connect/kafka_connectors.html)
* [Monitor Kafka Connect](https://docs.confluent.io/platform/current/connect/monitoring.html)
* [Install connector plugins with CFK](https://docs.confluent.io/operator/current/co-connectors.html)

In distributed mode, connectors are created and managed through the Connect REST API. The API can be accessed through any worker, and requests are forwarded within the worker group when needed. ([Confluent Documentation][10])

## Lab 1: Deploy distributed Connect

Follow:

* [Deploy connectors using CFK](https://docs.confluent.io/operator/current/co-connectors.html)
* [Connect worker configuration](https://docs.confluent.io/platform/current/connect/references/allconfigs.html)

Verify:

```bash
curl -s http://<connect-host>:8083/ | jq
curl -s http://<connect-host>:8083/connector-plugins | jq
curl -s http://<connect-host>:8083/connectors | jq
```

The student must identify:

* Worker group ID.
* Configuration topic.
* Offset topic.
* Status topic.
* Replication factor of internal topics.
* Installed plugins.
* Converter configuration.

## Lab 2: Create a connector

Follow:

* [Configure connectors in distributed mode](https://docs.confluent.io/platform/current/connect/configuring.html)
* [Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @connector.json \
  http://<connect-host>:8083/connectors
```

Inspect:

```bash
curl -s \
  http://<connect-host>:8083/connectors/<connector-name>/status \
  | jq
```

## Lab 3: Connector lifecycle

The student must:

1. Create a connector.
2. Update its configuration.
3. Pause it.
4. Resume it.
5. Restart the connector.
6. Restart one failed task.
7. Delete the connector.
8. Verify connector offsets.
9. Restart a Connect worker.
10. Verify task reassignment.

Relevant references:

* [Connect REST API endpoint reference](https://docs.confluent.io/platform/current/connect/references/restapi.html)
* [Connector and task monitoring](https://docs.confluent.io/platform/current/connect/monitoring.html)

## Lab 4: JDBC connector

Use:

* [JDBC Source Connector](https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/overview.html)
* [JDBC Source Connector configuration](https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html)
* [JDBC Sink Connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
* [JDBC Sink Connector configuration](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/sink_config_options.html)

The student must test:

* Bulk mode.
* Incrementing-column mode.
* Timestamp mode.
* Timestamp-plus-incrementing mode.
* Table allowlists.
* Schema changes.
* Database disconnection.
* Invalid database credentials.
* Duplicate records.
* Sink primary-key handling.

## Lab 5: Oracle and DB2 connectors

Where these are used in the enterprise, the student must review the exact installed connector documentation and version.

Starting references:

* [Confluent connector documentation](https://docs.confluent.io/kafka-connectors/index.html)
* [Oracle Database CDC Source Connector](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)
* [IBM MQ Source Connector](https://docs.confluent.io/kafka-connectors/ibmmq-source/current/overview.html)
* [JDBC Connector](https://docs.confluent.io/kafka-connectors/jdbc/current/overview.html)

For DB2 database access through JDBC, validate:

* Supported DB2 driver.
* Driver licensing.
* Database privileges.
* Incremental capture strategy.
* Data-type compatibility.
* Transaction-isolation implications.
* Network encryption.
* Driver availability in the Connect image.

## Lab 6: Error handling

Follow:

* [Kafka Connect error handling](https://docs.confluent.io/platform/current/connect/references/allconfigs.html)
* [Monitor Connect and connectors](https://docs.confluent.io/platform/current/connect/monitoring.html)

Configure and test:

```properties
errors.tolerance=all
errors.retry.timeout=600000
errors.retry.delay.max.ms=30000
errors.deadletterqueue.topic.name=<dlq-topic>
errors.deadletterqueue.context.headers.enable=true
errors.log.enable=true
errors.log.include.messages=true
```

The student must diagnose:

* Database connection failure.
* Authentication failure.
* Kafka authorization failure.
* Schema Registry failure.
* Serialization failure.
* Invalid record.
* Connector-plugin failure.
* Worker failure.
* Offset issue.
* Dead-letter queue growth.

---

# 14. Phase 10: Schema Registry

## Objective

Manage schemas as production data contracts.

## Required learning material

* [Schema Registry overview](https://docs.confluent.io/platform/current/schema-registry/index.html)
* [Schema Registry course](https://developer.confluent.io/courses/apache-kafka/schema-registry/)
* [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
* [Avro, JSON Schema, and Protobuf serializers](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
* [Schema Registry API reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
* [Schema Registry API examples](https://docs.confluent.io/platform/current/schema-registry/develop/using.html)
* [Schema references](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
* [Data contracts](https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html)
* [Schema Registry multi-tenancy](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-registry-multi-tenancy.html)

Schema Registry provides schema versioning, validation, evolution, and compatibility enforcement. Its REST API supports registering schemas, retrieving versions, and managing compatibility levels. ([Confluent Documentation][11])

## Lab 1: Register a schema

Follow:

* [Schema Registry API usage examples](https://docs.confluent.io/platform/current/schema-registry/develop/using.html)

Example:

```bash
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"}]}"}' \
  http://<schema-registry-host>:8081/subjects/orders-value/versions
```

List subjects:

```bash
curl -s \
  http://<schema-registry-host>:8081/subjects \
  | jq
```

Retrieve the latest version:

```bash
curl -s \
  http://<schema-registry-host>:8081/subjects/orders-value/versions/latest \
  | jq
```

## Lab 2: Compatibility

Follow:

* [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)

The student must test:

* `BACKWARD`.
* `BACKWARD_TRANSITIVE`.
* `FORWARD`.
* `FORWARD_TRANSITIVE`.
* `FULL`.
* `FULL_TRANSITIVE`.
* `NONE`.

Test compatibility before registration:

```bash
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @new-schema.json \
  http://<schema-registry-host>:8081/compatibility/subjects/orders-value/versions/latest
```

## Lab 3: Breaking and nonbreaking changes

The student must demonstrate:

* Adding an optional field with a default.
* Removing a required field.
* Changing a field type.
* Renaming a field.
* Adding an enum symbol.
* Removing an enum symbol.
* Introducing a schema reference.
* Producing data with an older schema.
* Consuming data with a newer schema.

## Lab 4: Subject naming

Follow:

* [Schema subject-name strategies](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)

Compare:

* `TopicNameStrategy`.
* `RecordNameStrategy`.
* `TopicRecordNameStrategy`.

The student must select and document the organization’s standard.

## Lab 5: Schema Registry troubleshooting

Diagnose:

* HTTP 401.
* HTTP 403.
* HTTP 409.
* HTTP 422.
* Unavailable Kafka backing topic.
* TLS trust failure.
* Invalid schema.
* Incompatible schema.
* Serializer configuration failure.
* Incorrect subject name.
* Schema ID not found.

---

# 15. Phase 11: Monitoring and Observability

## Objective

Build the ability to detect failures before users report them.

## Required learning material

* [Monitor Confluent Platform with CFK](https://docs.confluent.io/operator/current/co-monitor-cp.html)
* [Kafka JMX monitoring](https://docs.confluent.io/platform/current/kafka/monitoring.html)
* [Configure JMX](https://docs.confluent.io/platform/current/kafka/configure-jmx.html)
* [Monitor Kafka Connect](https://docs.confluent.io/platform/current/connect/monitoring.html)
* [OpenShift monitoring](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/monitoring/)
* [Prometheus documentation](https://prometheus.io/docs/introduction/overview/)
* [Grafana documentation](https://grafana.com/docs/grafana/latest/)

CFK exposes Confluent component JMX metrics and provides a Prometheus exporter endpoint on port 7778. Access to the exporter should be controlled through network policy because the CFK exporter endpoint does not provide authentication or encryption. ([Confluent Documentation][12])

## Kafka metrics to learn

* Active controller count.
* Offline partitions count.
* Under-replicated partitions.
* Under-minimum-ISR partitions.
* Leader election rate.
* Unclean leader elections.
* Produce request rate.
* Fetch request rate.
* Request latency.
* Request queue time.
* Network processor idle percentage.
* Request handler idle percentage.
* Bytes in.
* Bytes out.
* Replica-fetcher lag.
* JVM heap.
* Garbage collection.
* Disk utilization.
* Consumer lag.

## Connect metrics

* Connector status.
* Task status.
* Source-record poll rate.
* Source-record write rate.
* Sink-record read rate.
* Sink-record active count.
* Batch size.
* Retry count.
* Dead-letter queue volume.
* Offset commit success.
* Worker rebalances.

## OpenShift metrics

* Pod readiness.
* Restart count.
* CPU throttling.
* Memory use.
* Out-of-memory kills.
* Node pressure.
* PVC capacity.
* Storage latency.
* Network errors.
* DNS failures.
* Operator reconciliation errors.

## Monitoring lab

Build alerts for:

1. Offline partition count greater than zero.
2. Under-replicated partitions persisting longer than five minutes.
3. Under-minimum-ISR partitions.
4. Consumer lag over the application threshold.
5. Broker disk above 75%.
6. Broker disk above 85%.
7. PVC projected exhaustion.
8. Failed Connect task.
9. Schema Registry unavailable.
10. Certificate expiration within 60 days.
11. Certificate expiration within 30 days.
12. Repeated authentication failures.
13. Cluster-link lag over the DR threshold.
14. OpenShift pod restart storm.
15. Node memory or disk pressure.

---

# 16. Phase 12: Structured Troubleshooting

## Objective

Use a consistent methodology rather than restarting components prematurely.

## Troubleshooting order

1. Identify the environment.
2. Identify the cluster.
3. Identify the tenant.
4. Identify the affected client or component.
5. Establish when the problem started.
6. Identify recent changes.
7. Test DNS.
8. Test TCP connectivity.
9. Test TLS negotiation.
10. Test authentication.
11. Test authorization.
12. Review Kafka metadata.
13. Review topic and partition health.
14. Review consumer groups.
15. Review connector or Schema Registry status.
16. Review pod conditions and OpenShift events.
17. Review node conditions.
18. Review storage.
19. Review east-west network conditions.
20. Preserve evidence before restarting anything.

## Kafka troubleshooting references

* [Kafka monitoring](https://docs.confluent.io/platform/current/kafka/monitoring.html)
* [Kafka CLI tools](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)
* [Topic operations](https://docs.confluent.io/kafka/operations-tools/topic-operations.html)
* [Consumer-group operations](https://docs.confluent.io/kafka/operations-tools/manage-consumer-groups.html)

## CFK troubleshooting references

* [CFK troubleshooting](https://docs.confluent.io/operator/current/co-troubleshooting.html)
* [CFK monitoring](https://docs.confluent.io/operator/current/co-monitor-cp.html)
* [CFK custom-resource API](https://docs.confluent.io/operator/current/co-api.html)

## OpenShift troubleshooting references

* [OpenShift troubleshooting](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/support/troubleshooting)
* [Gathering cluster data](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/support/gathering-cluster-data)
* [OVN-Kubernetes troubleshooting](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/ovn-kubernetes_network_plugin/ovn-kubernetes-troubleshooting-sources)

## Incident lab scenarios

The instructor should introduce:

* One failed broker.
* One failed KRaft controller.
* One full PVC.
* One invalid certificate.
* One missing ACL.
* One failed NetworkPolicy.
* One unavailable Schema Registry pod.
* One failed connector task.
* One slow database.
* One consumer-lag incident.
* One data-center link degradation.

The student must produce:

* Incident timeline.
* Evidence collected.
* Root cause.
* Restoration actions.
* Data-integrity validation.
* Corrective action.
* Preventive action.
* Runbook update.

---

# 17. Phase 13: East-West Multi-Data-Center Architecture

## Objective

Understand and operate the selected multi-data-center architecture.

The organization must first determine whether east and west are:

1. One stretched Multi-Region Cluster.
2. Two independent clusters connected by Cluster Linking.
3. Two clusters connected by Replicator.
4. A migration from an older replication pattern to Cluster Linking.

Confluent documents multiple multi-data-center patterns for disaster recovery, migration, locality, data sharing, and active-active operation. ([Confluent Documentation][13])

---

## Option A: Multi-Region Cluster

A Multi-Region Cluster is one Kafka cluster distributed across locations. It supports replica-placement policies and follower fetching, but its quorum, latency, and failure characteristics must be carefully engineered. ([Confluent Documentation][14])

### Learning material

* [Multi-data-center architecture overview](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region-architectures.html)
* [Configure Multi-Region Clusters](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region.html)
* [Multi-Region Cluster tutorial](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region-tutorial.html)
* [Deploy a Multi-Region Cluster with CFK](https://docs.confluent.io/operator/current/co-multi-region.html)
* [Troubleshoot Multi-Region Clusters](https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region.html)

### Skills

* Controller placement.
* Broker placement.
* Rack identification.
* Replica-placement constraints.
* Observer replicas.
* Synchronous versus asynchronous topics.
* Follower fetching.
* Cross-data-center latency.
* Quorum loss.
* Site isolation.
* Data availability during WAN failure.

### Lab

1. Define east and west rack IDs.
2. Configure broker placement.
3. Create a synchronous topic.
4. Create an asynchronous topic.
5. Verify replica placement.
6. Simulate loss of an east broker.
7. Simulate loss of the east data center.
8. Record topic availability.
9. Record write behavior.
10. Record consumer behavior.
11. Verify recovery after the site returns.

---

## Option B: Two clusters using Cluster Linking

Cluster Linking connects independent clusters and creates mirror topics on the destination. It supports disaster recovery, data sharing, migration, and multi-region patterns. ([Confluent Documentation][15])

### Learning material

* [Cluster Linking overview](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.html)
* [Cluster Linking configuration](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/configs.html)
* [Cluster Linking commands](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/commands.html)
* [Cluster Linking security](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/security.html)
* [Cluster Linking monitoring](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/monitoring.html)
* [Cluster Linking concepts and mirror topics](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.html)
* [Cluster Linking FAQ](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/faqs-cp.html)
* [Schema Linking](https://docs.confluent.io/platform/current/schema-registry/schema-linking-cp.html)

Cluster links and mirror topics can be administered through CLI commands or the Kafka REST v3 interface. ([Confluent Documentation][16])

### Skills

* Source and destination clusters.
* Cluster-link credentials.
* Mirror-topic creation.
* Topic-configuration synchronization.
* Offset synchronization.
* Mirror-topic promotion.
* Link lag.
* Failover.
* Failback.
* Reverse links.
* Split-brain prevention.
* Schema synchronization.
* Client-bootstrap switching.

### Lab 1: Create a cluster link

Follow:

* [Cluster Linking commands](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/commands.html)

The student must:

1. Create an east-to-west cluster link.
2. Verify link status.
3. Create a mirror topic.
4. Produce data in east.
5. Consume mirrored data in west.
6. Measure replication lag.
7. Compare topic configurations.
8. Verify consumer-offset behavior.

### Lab 2: Failover

Follow the current Confluent Cluster Linking disaster-recovery guidance and organization-specific runbook.

Required steps:

1. Declare east unavailable.
2. Stop or fence east-side producers.
3. Confirm final replication status.
4. Promote west mirror topics.
5. Activate west connectors.
6. Update application bootstrap routing.
7. Start west-side producers.
8. Resume consumers.
9. Validate schemas.
10. Validate offsets.
11. Validate data completeness.
12. Record RTO and RPO.

### Lab 3: Failback

1. Restore east.
2. Prevent applications from writing to east prematurely.
3. Establish the reverse replication path.
4. Synchronize data.
5. Validate offsets and topic configurations.
6. Stop west writes.
7. Perform controlled application cutback.
8. Validate east.
9. Resume the normal replication configuration.
10. Document any lost, duplicated, or replayed records.

---

## Option C: Confluent Replicator

### Learning material

* [Replicator overview](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/index.html)
* [Replicator failover](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/replicator-failover.html)
* [Replicator configuration](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/replicator-config-options.html)
* [Replicator monitoring](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/replicator-monitoring.html)

This material is required when Replicator already exists in the environment. For new designs, the architecture team should evaluate Cluster Linking against the organization’s requirements and licensed Confluent capabilities.

---

# 18. Disaster-Recovery Decision Document

The student must produce a document answering:

* Is the topology one cluster or two?
* Which site is normally active?
* Can both sites accept writes?
* Which topics replicate?
* Which topics are synchronous?
* Which topics are asynchronous?
* What is the RPO for each topic class?
* What is the RTO for each application class?
* How are producer writes fenced during failover?
* How do clients change bootstrap servers?
* Is DNS used?
* Is application configuration changed?
* Are both bootstrap lists already configured?
* How are connectors activated in the recovery site?
* How are schemas replicated?
* How are consumer offsets handled?
* Who declares the disaster?
* Who authorizes promotion?
* How is failback approved?
* How is split-brain operation prevented?
* How is data integrity validated?

---

# 19. Phase 14: Development, Test, and Production Operations

## Development

Use development for:

* New connector testing.
* Schema experimentation.
* Client testing.
* Basic security testing.
* Topic-policy development.

Development must not contain unmasked production-sensitive data.

## Test

Test should be production-representative for:

* Authentication.
* Authorization.
* Certificate rotation.
* Connector upgrades.
* Schema compatibility.
* Broker upgrades.
* OpenShift upgrades.
* Load testing.
* Failover testing.
* Recovery testing.

## Production

Production requires:

* Change approval.
* Peer review.
* Git-controlled configuration.
* Backup of configuration state.
* Maintenance windows where required.
* Prechange health validation.
* Postchange validation.
* Rollback criteria.
* Incident communications.
* Audit evidence.

## Configuration-management references

* [Helm documentation](https://helm.sh/docs/)
* [Kustomize documentation](https://kubectl.docs.kubernetes.io/guides/introduction/kustomize/)
* [OpenShift GitOps documentation](https://docs.redhat.com/en/documentation/red_hat_openshift_gitops/)
* [Argo CD documentation](https://argo-cd.readthedocs.io/)
* [CFK configuration](https://docs.confluent.io/operator/current/co-configure.html)

## Recommended repository structure

```text
confluent-platform/
├── base/
│   ├── kraft/
│   ├── kafka/
│   ├── schema-registry/
│   ├── connect/
│   └── monitoring/
├── environments/
│   ├── dev/
│   ├── test/
│   └── production/
├── tenants/
├── topics/
├── connectors/
├── schemas/
├── security/
├── alerts/
└── runbooks/
```

---

# 20. Phase 15: Upgrades

## Objective

Upgrade CFK and Confluent Platform without avoidable interruption.

## Required learning material

* [CFK upgrade overview](https://docs.confluent.io/operator/current/co-upgrade-overview.html)
* [Upgrade CFK](https://docs.confluent.io/operator/current/co-upgrade-cfk.html)
* [Upgrade Confluent Platform with CFK](https://docs.confluent.io/operator/current/co-upgrade-cp.html)
* [Confluent Platform upgrade checklist](https://docs.confluent.io/platform/current/installation/upgrade-checklist.html)
* [Confluent Platform release notes](https://docs.confluent.io/platform/current/release-notes/index.html)
* [CFK release notes](https://docs.confluent.io/operator/current/release-notes.html)
* [OpenShift updating clusters](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/updating_clusters/)

Confluent recommends separating CFK upgrades from unrelated configuration changes, scaling operations, and credential rotations. This reduces the number of variables involved in troubleshooting and rollback. ([Confluent Documentation][17])

## Upgrade lab

The student must:

1. Verify the compatibility matrix.
2. Review release notes.
3. Export existing custom resources.
4. Record cluster health.
5. Record offline and under-replicated partitions.
6. Confirm storage capacity.
7. Confirm connector health.
8. Confirm Schema Registry health.
9. Upgrade CFK in test.
10. Upgrade Confluent components in test.
11. Validate clients.
12. Validate connectors.
13. Validate schemas.
14. Validate security.
15. Validate east-west replication.
16. Document rollback limitations.
17. Prepare the production change.
18. Execute production upgrade.
19. Complete post-upgrade validation.

Do not combine the following in a single production change unless specifically required:

* CFK upgrade.
* Confluent Platform upgrade.
* OpenShift upgrade.
* Certificate rotation.
* Broker scaling.
* Storage migration.
* Authentication change.
* Authorization redesign.

---

# 21. Day-to-Day Operational Curriculum

## Daily checks

The student must learn to review:

* CFK resource status.
* Operator errors.
* Broker readiness.
* Controller readiness.
* Offline partitions.
* Under-replicated partitions.
* Under-minimum-ISR partitions.
* Consumer lag.
* Failed connector tasks.
* Schema Registry availability.
* Pod restarts.
* Node pressure.
* Storage utilization.
* Authentication failures.
* Authorization failures.
* East-west replication lag.
* Certificate-expiration alerts.

## Weekly checks

* Topic growth.
* Tenant utilization.
* Consumer-lag trends.
* Connector reliability.
* Dead-letter queue growth.
* Database connector errors.
* OpenShift events.
* Persistent-volume growth.
* Failed login patterns.
* Unused service accounts.
* Cluster-link status.
* Outstanding platform changes.

## Monthly checks

* Capacity forecast.
* Storage forecast.
* Certificate-expiration report.
* User access review.
* Service-account review.
* Topic and connector inventory.
* Unused topic review.
* Schema governance review.
* Patch and upgrade review.
* DR runbook review.
* Monitoring coverage review.

## Quarterly checks

* Broker failure test.
* Controller failure test.
* Worker-node drain test.
* Certificate-rotation exercise.
* Connector-recovery test.
* Schema Registry recovery test.
* East-west failover exercise.
* East-west failback exercise.
* Access certification.
* Capacity and architecture review.

---

# 22. Required Runbooks

Each student should write and test the following:

1. Broker pod failure.
2. KRaft controller failure.
3. OpenShift worker-node failure.
4. PVC failure.
5. Full Kafka disk.
6. NetworkPolicy failure.
7. DNS failure.
8. Broker TLS certificate expiration.
9. Client certificate expiration.
10. Certificate rotation.
11. OIDC provider outage.
12. Invalid OIDC token.
13. Kafka authorization failure.
14. Consumer lag.
15. Under-replicated partitions.
16. Offline partitions.
17. Schema Registry outage.
18. Incompatible schema submission.
19. Connect worker failure.
20. Connector task failure.
21. Source database outage.
22. Sink database outage.
23. Poison record.
24. Dead-letter queue investigation.
25. Cluster-link failure.
26. East-to-west failover.
27. West-to-east failback.
28. CFK upgrade.
29. Confluent Platform upgrade.
30. OpenShift upgrade impact.
31. Confluent support-bundle collection.
32. OpenShift `must-gather` collection.

Each runbook must include:

* Purpose.
* Scope.
* Trigger.
* Severity.
* Prerequisites.
* Required access.
* Diagnostic commands.
* Evidence to preserve.
* Decision points.
* Recovery steps.
* Validation steps.
* Rollback.
* Escalation.
* Communications.
* Postincident actions.

---

# 23. Capstone Exercise

## Build

The student must build:

* Development environment.
* Test environment.
* Production-representative environment.
* KRaft controllers.
* Three or more Kafka brokers.
* Persistent storage.
* Pod anti-affinity.
* Rack awareness.
* Schema Registry.
* Distributed Kafka Connect.
* Two connectors.
* Two tenants.
* mTLS client authentication.
* OIDC workload authentication.
* RBAC or ACL authorization.
* Avro or Protobuf schemas.
* Monitoring and alerts.
* East-west replication.

## Injected incidents

The instructor introduces:

* Failed broker.
* Failed controller.
* Full volume.
* Incorrect certificate.
* Expired certificate.
* Missing ACL.
* Invalid OIDC audience.
* Failed connector task.
* Breaking schema.
* NetworkPolicy block.
* Consumer-lag spike.
* Loss of the east site.

## Passing criteria

The student must:

* Identify the correct failure domain.
* Collect evidence.
* Avoid unnecessary restarts.
* Restore service through an approved runbook.
* Verify data integrity.
* Verify authorization boundaries.
* Record RPO and RTO.
* Complete failover.
* Complete failback.
* Produce a root-cause analysis.

---

# 24. Production-Readiness Skills Matrix

| Skill              | Foundation                | Working Knowledge               | Production Ready                                      |
| ------------------ | ------------------------- | ------------------------------- | ----------------------------------------------------- |
| Kafka architecture | Explain components        | Administer topics and groups    | Diagnose quorum and replication failures              |
| OpenShift          | Navigate resources        | Deploy stateful workloads       | Diagnose node, storage, DNS, and networking failures  |
| CFK                | Understand operators      | Deploy Confluent components     | Troubleshoot reconciliation and upgrades              |
| KRaft              | Explain controllers       | Inspect quorum health           | Recover from controller and quorum incidents          |
| TLS/mTLS           | Explain PKI               | Configure clients and listeners | Rotate trust and identity without interruption        |
| OIDC               | Explain tokens            | Configure users and workloads   | Diagnose claims, signatures, and IdP outages          |
| RBAC/ACLs          | Explain authorization     | Grant resource access           | Design least-privilege enterprise controls            |
| Multi-tenancy      | Explain separation        | Onboard tenants                 | Enforce isolation, governance, and quotas             |
| Kafka Connect      | Explain workers and tasks | Deploy connectors               | Diagnose plugins, offsets, data, and external systems |
| Schema Registry    | Explain subjects          | Manage versions                 | Govern compatibility and data contracts               |
| Monitoring         | Read metrics              | Respond to alerts               | Tune thresholds and forecast capacity                 |
| Multi-data-center  | Explain topology          | Monitor replication             | Execute failover and failback                         |
| Upgrades           | Explain rolling upgrades  | Upgrade test                    | Lead controlled production upgrades                   |
| Incident response  | Follow a runbook          | Diagnose common failures        | Lead major incidents and root-cause analysis          |

---

# 25. Recommended Certification and Formal Training

## Confluent

* [Confluent training and certification](https://www.confluent.io/training/)
* [Confluent Developer courses](https://developer.confluent.io/courses/)
* Confluent Certified Administrator for Apache Kafka.
* Confluent Certified Developer for Apache Kafka.

## Red Hat

* [Red Hat training and certification](https://www.redhat.com/en/services/training-and-certification)
* Red Hat System Administration.
* Red Hat OpenShift Administration.
* Red Hat Certified Specialist in OpenShift Administration.
* Red Hat OpenShift troubleshooting and operations training.

Certification should supplement hands-on qualification. It should not replace the capstone, failover exercise, or production-readiness evaluation.

---

# 26. Required Training Deliverables

Each student should finish the program with:

1. Platform architecture diagram.
2. Network-flow diagram.
3. Certificate and trust-flow diagram.
4. OIDC authentication-flow diagram.
5. Authorization model.
6. Tenant onboarding checklist.
7. Topic naming standard.
8. Consumer-group naming standard.
9. Schema subject naming standard.
10. Connector deployment standard.
11. Environment promotion process.
12. Monitoring dashboard.
13. Alert catalog.
14. Capacity model.
15. East-west failover runbook.
16. East-west failback runbook.
17. Certificate-rotation runbook.
18. Platform upgrade runbook.
19. Incident troubleshooting checklist.
20. Production-readiness assessment.

The engineer should not receive production administrative access solely for completing reading material. Production access should require successful completion of the labs, incident scenarios, security exercises, and capstone failover.