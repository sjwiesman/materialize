---
title: "Kafka with AWS Glue Schema Registry"
description: "How to export results from Materialize to Kafka as Avro, with schemas managed in AWS Glue Schema Registry."
menu:
  main:
    parent: sink
    name: "Kafka with AWS Glue Schema Registry"
    weight: 26
---

{{< public-preview />}}

This guide walks you through exposing the results of a view in Materialize as
a stream of Avro-encoded Kafka messages whose schemas are registered in [AWS
Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).
Downstream consumers, such as Kafka consumers, Kafka Connect, Amazon Managed
Service for Apache Flink, or AWS Lambda, can then decode the messages using the
AWS Glue Schema Registry serializer/deserializer libraries.

Materialize generates the Avro schemas from the view's columns, registers them
in your Glue registry, and continuously writes every change to the view's
results into the Kafka topic.

## Prerequisites

- A Kafka cluster that Materialize can reach, for example an [Amazon
  MSK](https://aws.amazon.com/msk/) cluster. See [`CREATE CONNECTION`: Kafka](/sql/create-connection/#kafka)
  for the supported authentication and networking options.
- An AWS account with permissions to create and manage AWS Glue registries,
  IAM policies, and IAM roles.
- The view you want to expose. Sinks read from a [materialized
  view](/sql/create-materialized-view/), a table, or a source. If your results
  are defined in a regular view, you create a materialized view over it in
  [Step 6](#step-6-materialize-the-view).

## Configure AWS

### Step 1. Create a Glue registry

Materialize creates and versions schemas within a registry but never creates
the registry itself. If you don't already have one, create a registry in the
same region as the AWS connection you create in [Step 3](#step-3-create-an-aws-connection-in-materialize).
For example, using the AWS CLI:

```bash
aws glue create-registry \
    --registry-name materialize-sinks \
    --region <region>
```

You can also use an existing registry, including the `default-registry` that
AWS Glue creates in each account and region.

### Step 2. Set up permissions in AWS

This guide uses an IAM policy and an IAM role that Materialize assumes. We
**strongly** recommend [role assumption-based
authentication](/sql/create-connection/#aws-permissions) over access keys.

#### Step 2A. Create an IAM policy

Create an [IAM
policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that allows Materialize to register schemas in your registry. Replace
`<region>`, `<account>`, and `<registry-name>`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetRegistry",
                "glue:GetSchemaVersion",
                "glue:GetSchemaByDefinition",
                "glue:RegisterSchemaVersion",
                "glue:CreateSchema",
                "glue:GetSchema"
            ],
            "Resource": [
                "arn:aws:glue:<region>:<account>:registry/<registry-name>",
                "arn:aws:glue:<region>:<account>:schema/<registry-name>/*"
            ]
        }
    ]
}
```

For what each action is used for, see [AWS Glue Schema Registry
permissions](/sql/create-connection/#glue-permissions).

#### Step 2B. Create an IAM role

Create an [IAM
role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) that
Materialize can assume.

1. For the **Trusted entity type**, specify **Custom trust policy** with the
   following:
    - `Principal`: The example uses the [Materialize Cloud IAM
      principal](/sql/create-connection/#aws-permissions). For self-managed
      deployments and the Emulator, the principal will differ.
    - `ExternalId`: `"PENDING"` is a placeholder that you replace in [Step
      4](#step-4-update-the-iam-role-in-aws).

    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": "PENDING"
                    }
                }
            }
        ]
    }
    ```

1. For permissions, attach the [IAM policy created
   earlier](#step-2a-create-an-iam-policy).

Once you have created the IAM role, copy its ARN. You use it in the next step.

## Configure Materialize

### Step 3. Create an AWS connection in Materialize

1. Use [`CREATE CONNECTION ... TO AWS`](/sql/create-connection/#aws) to create
   an AWS connection, replacing `<IAM role ARN>` with the ARN from [Step
   2B](#step-2b-create-an-iam-role) and `<region>` with the region of your
   Glue registry:

    ```mzsql
    CREATE CONNECTION aws_connection TO AWS (
        ASSUME ROLE ARN = '<IAM role ARN>',
        REGION = '<region>'
    );
    ```

1. Fetch the external ID for the connection:

    ```mzsql
    SELECT external_id
    FROM mz_internal.mz_aws_connections awsc
    JOIN mz_connections c ON awsc.id = c.id
    WHERE c.name = 'aws_connection';
    ```

### Step 4. Update the IAM role in AWS

Update the trust policy of the IAM role created in [Step
2B](#step-2b-create-an-iam-role), replacing `"PENDING"` with the external ID
from the previous step:

```json{hl_lines="12"}
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "mz_1234abcd-5678-efgh-9012-ijklmnopqrst_u123"
                }
            }
        }
    ]
}
```

To confirm that Materialize can assume the role, run:

```mzsql
VALIDATE CONNECTION aws_connection;
```

### Step 5. Create the Glue and Kafka connections

1. Create an [AWS Glue Schema Registry
   connection](/sql/create-connection/#aws-glue-schema-registry) that points at
   your registry and authenticates using the AWS connection:

    ```mzsql
    CREATE CONNECTION glue_connection TO AWS GLUE SCHEMA REGISTRY (
        AWS CONNECTION = aws_connection,
        REGISTRY = 'materialize-sinks'
    );
    ```

    By default, Materialize validates the connection by calling
    `glue:GetRegistry` when you create it.

1. If you don't already have one, create a [Kafka
   connection](/sql/create-connection/#kafka). For example, for an Amazon MSK
   cluster that uses IAM authentication, you can reuse an AWS connection
   whose role also grants the required MSK permissions:

    ```mzsql
    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker>:9098',
        SECURITY PROTOCOL = 'SASL_SSL',
        AWS CONNECTION = aws_connection
    );
    ```

    For other authentication methods and private networking, see [`CREATE
    CONNECTION`: Kafka](/sql/create-connection/#kafka).

### Step 6. Materialize the view

A sink writes out every change to its upstream relation, so the relation must
be one that Materialize maintains: a materialized view, a table, or a source.
If your results are defined in a regular view, create a materialized view over
it. For example, given a view `order_totals`:

```mzsql
CREATE VIEW order_totals AS
    SELECT customer_id, count(*) AS order_count, sum(amount) AS total_amount
    FROM orders
    GROUP BY customer_id;

CREATE MATERIALIZED VIEW order_totals_mv
    IN CLUSTER my_compute_cluster
    AS SELECT * FROM order_totals;
```

## Create the sink

### Step 7. Create the Kafka sink

Use [`CREATE SINK`](/sql/create-sink/kafka) with `FORMAT AVRO USING AWS GLUE
SCHEMA REGISTRY CONNECTION`. We recommend running sinks on a dedicated cluster,
separate from the clusters that host your sources and compute.

{{< tabs >}}
{{< tab "Upsert envelope" >}}

The [upsert envelope](/sql/create-sink/kafka#upsert-envelope) writes the latest
value for each key, and a tombstone when a key is deleted. Use it when
consumers want the current state of the view, for example to maintain a table
or a compacted topic.

```mzsql
CREATE SINK order_totals_sink
    IN CLUSTER my_sink_cluster
    FROM order_totals_mv
    INTO KAFKA CONNECTION kafka_connection (TOPIC 'order-totals')
    KEY (customer_id)
    FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_connection
    ENVELOPE UPSERT;
```

The key columns must uniquely identify a row of the view. See [upsert key
selection](/sql/create-sink/kafka#upsert-key-selection) for details.

{{< /tab >}}
{{< tab "Debezium envelope" >}}

The [Debezium envelope](/sql/create-sink/kafka#debezium-envelope) writes each
change as a record with `before` and `after` fields. Use it when consumers need
to see how each row changed.

```mzsql
CREATE SINK order_totals_sink
    IN CLUSTER my_sink_cluster
    FROM order_totals_mv
    INTO KAFKA CONNECTION kafka_connection (TOPIC 'order-totals')
    FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_connection
    ENVELOPE DEBEZIUM;
```

{{< /tab >}}
{{< /tabs >}}

When the sink starts, Materialize registers the value schema in the registry
under the name `<topic>-value` and, if the sink has a `KEY`, the key schema
under `<topic>-key`. In the example above, the schemas are `order-totals-value`
and `order-totals-key`. If a schema with that name already exists, Materialize
reuses an identical version or registers a new version.

#### Optional: set schema names and compatibility levels

To choose the schema names, or the compatibility level applied when Materialize
creates a schema, pass options to the Glue connection in the `FORMAT` clause:

```mzsql
CREATE SINK order_totals_sink
    IN CLUSTER my_sink_cluster
    FROM order_totals_mv
    INTO KAFKA CONNECTION kafka_connection (TOPIC 'order-totals')
    KEY (customer_id)
    FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_connection (
        KEY SCHEMA NAME = 'order-totals-key',
        VALUE SCHEMA NAME = 'order-totals-value',
        KEY COMPATIBILITY LEVEL = 'FULL',
        VALUE COMPATIBILITY LEVEL = 'BACKWARD'
    )
    ENVELOPE UPSERT;
```

Compatibility levels use the Confluent names and default to `BACKWARD`. They
apply only when Materialize creates the schema. An existing schema keeps its
compatibility level. For how each level maps to its AWS Glue equivalent, see
[Compatibility levels](/sql/create-sink/kafka#compatibility-levels).

### Step 8. Verify the sink

1. Check that the sink is running:

    ```mzsql
    SELECT name, status, error
    FROM mz_internal.mz_sink_statuses
    WHERE name = 'order_totals_sink';
    ```

    Materialize publishes schemas when the sink starts running, not when you
    run `CREATE SINK`. Registry errors, such as missing IAM permissions or a
    new schema version that violates the existing schema's compatibility
    level, appear in the `error` column.

1. Check that the schemas were registered in Glue:

    ```bash
    aws glue list-schemas \
        --registry-id RegistryName=materialize-sinks \
        --region <region>
    ```

## Consume the topic

Messages use the AWS Glue Schema Registry wire format: an 18-byte header
followed by the Avro-encoded datum. The header consists of a version byte
(`0x03`), a compression byte (`0x00`, uncompressed), and the 16-byte UUID of
the schema version used to encode the message. This framing differs from the
Confluent Schema Registry wire format, so consumers must use a Glue-aware
deserializer.

For Java consumers, use the [AWS Glue Schema Registry
library](https://github.com/awslabs/aws-glue-schema-registry). For example,
configure a Kafka consumer with the Glue deserializer:

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<broker>:9098");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-totals-consumer");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    GlueSchemaRegistryKafkaDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    GlueSchemaRegistryKafkaDeserializer.class.getName());
props.put(AWSSchemaRegistryConstants.AWS_REGION, "<region>");
props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
    AvroRecordType.GENERIC_RECORD.getName());
```

The consumer's IAM principal needs `glue:GetSchemaVersion` on the registry's
schemas to resolve each message's schema-version UUID.

With the upsert envelope, a deleted key is written as a message with a `null`
value. With the Debezium envelope, a deletion is a record whose `after` field
is `null`.

## Considerations

### Schema evolution

A sink's Avro schemas are derived from the columns of its upstream relation.
If you change the shape of the view, for example by adding a column, and
recreate the sink, Materialize registers a new schema version. AWS Glue
accepts the version only if it satisfies the schema's compatibility level.
Otherwise, the sink reports an error in `mz_internal.mz_sink_statuses`.

Column names that are not valid Avro names are adjusted. See
[Avro](/sql/create-sink/kafka#avro) for the naming rules and the SQL to Avro
type mapping.

### Exactly-once delivery

Kafka sinks provide exactly-once processing guarantees. See [Exactly-once
processing](/sql/create-sink/kafka#exactly-once-processing).

### Limitations

- The registry must exist before you create the sink. Materialize never
  creates registries.
- The `AVRO KEY FULLNAME`, `AVRO VALUE FULLNAME`, `NULL DEFAULTS`, and `doc`
  options are not supported with AWS Glue Schema Registry.
- AWS Glue's `DISABLED` compatibility level is not supported.
- The JSON format does not use a schema registry. Only `FORMAT AVRO` registers
  schemas in AWS Glue.

## Troubleshooting

For general sink issues, see [Troubleshooting
sinks](/serve-results/sink/sink-troubleshooting/).

| Symptom | Likely cause |
|---------|--------------|
| `CREATE CONNECTION ... TO AWS GLUE SCHEMA REGISTRY` fails validation | The IAM role lacks `glue:GetRegistry`, the trust policy's external ID doesn't match, or the registry doesn't exist in the AWS connection's region. |
| The sink status shows an access denied error | The IAM role lacks one of the schema-write actions listed in [Step 2A](#step-2a-create-an-iam-policy). |
| The sink status shows a compatibility error | A new schema version for the view violates the existing schema's compatibility level in AWS Glue. |
| Consumers fail to decode messages | The consumer is using a Confluent deserializer. Use a Glue Schema Registry deserializer instead. |

## Related pages

- [`CREATE SINK`: Kafka](/sql/create-sink/kafka)
- [`CREATE CONNECTION`: AWS Glue Schema Registry](/sql/create-connection/#aws-glue-schema-registry)
- [`CREATE CONNECTION`: AWS](/sql/create-connection/#aws)
- [Kafka and Redpanda](/serve-results/sink/kafka/)
- [AWS Glue Schema Registry documentation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
