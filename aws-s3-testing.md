# Troubleshooting Amazon S3 Connectivity from an On-Prem OpenShift Cluster Using an AWS CLI Pod

Organizations frequently deploy OpenShift clusters within their own datacenters while leveraging Amazon S3 as a secure and scalable object storage service. In many environments, direct public access to S3 is not permitted. Instead, connectivity is routed through a private network path and restricted to approved source IP addresses before reaching a dedicated Amazon S3 VPC Endpoint.

When connectivity problems occur, determining whether the issue is related to DNS resolution, network routing, firewall policies, source IP restrictions, IAM permissions, or S3 bucket policies can be challenging. A structured troubleshooting methodology helps isolate each layer of the connection and quickly identify the source of a failure.

This article demonstrates how to deploy a temporary AWS CLI pod within an OpenShift namespace, securely mount AWS credentials from an OpenShift Secret, and perform a series of diagnostic tests to validate connectivity to Amazon S3 through a VPC Endpoint. The guide also includes reusable OpenShift YAML definitions and executable command examples that can be adapted for Development, Test, Stage, and Production environments.

The AWS CLI pod created in this guide is named:

```text
aws-cli-pod
```

AWS credentials are mounted into the container at:

```text
/credentials
```

By following the procedures in this guide, administrators can systematically verify authentication, DNS resolution, network connectivity, endpoint access, and object operations against Amazon S3 while maintaining a repeatable troubleshooting process across multiple OpenShift environments.

---

# Architecture Overview

```text
+--------------------------------------------------------------+
|                    On-Prem OpenShift Cluster                 |
|                                                              |
|  +----------------------+                                    |
|  | aws-cli-pod          |                                    |
|  |                      |                                    |
|  | /credentials         |                                    |
|  +----------+-----------+                                    |
|             |                                                |
+-------------|------------------------------------------------+
              |
              | HTTPS (TCP/443)
              |
      On-Prem Firewall / NAT
              |
              | Approved Egress IP
              |
      VPN / AWS Direct Connect / Private WAN
              |
+-------------|------------------------------------------------+
|                     AWS Network                             |
|                                                             |
|        Amazon S3 VPC Endpoint                              |
|                 |                                           |
|                 |                                           |
|             Amazon S3 Bucket                               |
+-------------------------------------------------------------+
```

---

# Environment Matrix

| Environment | OpenShift Namespace | AWS Region     | S3 Bucket        | S3 VPC Endpoint URL | Expected Egress IP  |
| ----------- | ------------------- | -------------- | ---------------- | ------------------- | ------------------- |
| Development | `<dev-namespace>`   | `<aws-region>` | `<dev-bucket>`   | `<dev-vpce-url>`    | `<dev-egress-ip>`   |
| Test        | `<test-namespace>`  | `<aws-region>` | `<test-bucket>`  | `<test-vpce-url>`   | `<test-egress-ip>`  |
| Stage       | `<stage-namespace>` | `<aws-region>` | `<stage-bucket>` | `<stage-vpce-url>`  | `<stage-egress-ip>` |
| Production  | `<prod-namespace>`  | `<aws-region>` | `<prod-bucket>`  | `<prod-vpce-url>`   | `<prod-egress-ip>`  |

---

# 1. Create the AWS Credentials Secret

Create a local credentials file.

```bash
cat > credentials <<'EOF'
[default]
aws_access_key_id = <access-key-id>
aws_secret_access_key = <secret-access-key>
EOF
```

Optional AWS configuration file:

```bash
cat > config <<'EOF'
[default]
region = <aws-region>
output = json
EOF
```

Create the OpenShift Secret.

```bash
oc create secret generic aws-cli-credentials \
  --from-file=credentials=./credentials \
  --from-file=config=./config \
  -n <namespace>
```

Verify the Secret.

```bash
oc get secret aws-cli-credentials -n <namespace>
```

---

# 2. Create the AWS CLI Pod

Create the file `aws-cli-pod.yaml`.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: aws-cli-pod
  namespace: <namespace>
  labels:
    app: aws-cli-pod
spec:
  restartPolicy: Never
  containers:
  - name: aws-cli
    image: amazon/aws-cli:latest
    command:
    - /bin/sh
    - -c
    - sleep 36000
    env:
    - name: AWS_SHARED_CREDENTIALS_FILE
      value: /credentials/credentials
    - name: AWS_CONFIG_FILE
      value: /credentials/config
    - name: AWS_REGION
      value: <aws-region>
    - name: AWS_DEFAULT_REGION
      value: <aws-region>
    volumeMounts:
    - name: credentials
      mountPath: /credentials
      readOnly: true
  volumes:
  - name: credentials
    secret:
      secretName: aws-cli-credentials
```

Deploy the pod.

```bash
oc apply -f aws-cli-pod.yaml
```

Verify it is running.

```bash
oc get pod aws-cli-pod -n <namespace>
```

If necessary, inspect the pod.

```bash
oc describe pod aws-cli-pod -n <namespace>
```

---

# 3. Connect to the Pod

```bash
oc rsh -n <namespace> aws-cli-pod
```

Verify the credentials mount.

```bash
ls -lah /credentials
```

Verify the AWS environment variables.

```bash
env | grep AWS
```

---

# 4. Validate AWS Credentials

```bash
aws sts get-caller-identity
```

Expected failures include:

```text
Unable to locate credentials
InvalidAccessKeyId
SignatureDoesNotMatch
AccessDenied
```

These indicate authentication or authorization issues rather than network connectivity problems.

---

# 5. Verify DNS Resolution

Resolve the VPC Endpoint.

```bash
nslookup <vpce-dns-name>
```

or

```bash
getent hosts <vpce-dns-name>
```

Example:

```bash
nslookup vpce-0123456789abcdef0.s3.us-east-1.vpce.amazonaws.com
```

If DNS resolution fails, investigate:

* OpenShift DNS configuration
* CoreDNS forwarding
* On-premises DNS
* Conditional forwarders
* AWS private hosted zones
* VPN or Direct Connect DNS forwarding

---

# 6. Test HTTPS Connectivity

```bash
curl -v https://<vpce-dns-name>
```

Receiving a `403 Forbidden` response indicates that HTTPS connectivity is working but the request is unsigned, which is expected.

Connection timeouts generally indicate firewall, routing, or network connectivity problems.

---

# 7. Test Amazon S3 Access

List objects in the bucket using the VPC Endpoint.

```bash
aws s3 ls s3://<bucket-name> \
    --endpoint-url https://<vpce-dns-name>
```

---

# 8. Upload a Test Object

```bash
echo "OpenShift S3 connectivity test" > s3-test.txt
```

Upload the file.

```bash
aws s3 cp s3-test.txt \
    s3://<bucket-name>/s3-test.txt \
    --endpoint-url https://<vpce-dns-name>
```

---

# 9. Download the Test Object

```bash
aws s3 cp \
    s3://<bucket-name>/s3-test.txt \
    ./s3-test.txt \
    --endpoint-url https://<vpce-dns-name>
```

Verify the contents.

```bash
cat s3-test.txt
```

---

# 10. Delete the Test Object

```bash
aws s3 rm \
    s3://<bucket-name>/s3-test.txt \
    --endpoint-url https://<vpce-dns-name>
```

---

# 11. Verify the On-Prem Egress IP

If outbound Internet access is available, verify the source IP.

```bash
curl https://checkip.amazonaws.com
```

The returned address should match the expected egress IP for the environment.

If Internet access is not available, work with the network team to confirm that traffic destined for the S3 VPC Endpoint leaves the OpenShift cluster using the approved egress IP address.

---

# 12. Verify OpenShift Network Policies

List network policies.

```bash
oc get networkpolicy -n <namespace>
```

Describe each policy.

```bash
oc describe networkpolicy -n <namespace>
```

Ensure outbound TCP/443 traffic is permitted to the S3 VPC Endpoint.

Example NetworkPolicy:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-s3-egress
spec:
  podSelector:
    matchLabels:
      app: aws-cli-pod
  policyTypes:
  - Egress
  egress:
  - to:
    - ipBlock:
        cidr: <vpc-endpoint-cidr>
    ports:
    - protocol: TCP
      port: 443
```

---

# 13. Verify AWS Configuration

Validate the following AWS-side configuration:

* Amazon S3 bucket policy
* IAM user or IAM role permissions
* S3 VPC Endpoint policy
* Security Group associated with the VPC Endpoint (if applicable)
* Network ACLs
* Network routing between the on-premises network and AWS

These components must all permit access from the approved on-premises egress IP address.

---

# 14. Enable AWS CLI Debug Logging

```bash
aws s3 ls s3://<bucket-name> \
    --endpoint-url https://<vpce-dns-name> \
    --debug
```

Review the output for:

* Endpoint URL
* DNS resolution
* TLS negotiation
* Authorization headers
* HTTP status codes
* AWS service responses

---

# Common Failure Scenarios

| Symptom                           | Likely Cause                                   |
| --------------------------------- | ---------------------------------------------- |
| Pod fails to start                | Invalid YAML, missing image, SCC restrictions  |
| Credentials not found             | Secret missing or not mounted correctly        |
| Unable to locate credentials      | Incorrect environment variables or mount path  |
| AccessDenied                      | IAM policy, bucket policy, or endpoint policy  |
| DNS resolution failure            | On-premises DNS configuration                  |
| Connection timeout                | Firewall, routing, VPN, Direct Connect, or NAT |
| Could not connect to endpoint URL | Incorrect VPC Endpoint URL                     |
| SignatureDoesNotMatch             | Incorrect credentials or AWS Region            |
| SSL handshake failure             | Certificate or network inspection device       |

---

# Cleanup

Delete the troubleshooting pod.

```bash
oc delete pod aws-cli-pod -n <namespace>
```

Delete the credentials Secret if it is no longer required.

```bash
oc delete secret aws-cli-credentials -n <namespace>
```

---

# Recommended Troubleshooting Workflow

1. Create the AWS credentials Secret.
2. Deploy the AWS CLI pod.
3. Verify the credentials mount.
4. Validate AWS authentication.
5. Verify DNS resolution.
6. Test HTTPS connectivity.
7. List the S3 bucket.
8. Upload a test object.
9. Download the test object.
10. Delete the test object.
11. Verify the approved egress IP.
12. Review OpenShift NetworkPolicies.
13. Validate AWS S3 policies and VPC Endpoint configuration.
14. Enable AWS CLI debug logging for detailed diagnostics.

Following this workflow provides a repeatable method for diagnosing Amazon S3 connectivity issues from an on-premises OpenShift cluster while ensuring that each layer—from credentials and DNS to networking and AWS authorization—is validated independently.