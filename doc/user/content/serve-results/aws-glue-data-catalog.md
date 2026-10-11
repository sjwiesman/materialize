---
title: "Expose views in AWS Glue Data Catalog"
description: "How to register Materialize views in the AWS Glue Data Catalog so JDBC clients can discover and query them."
menu:
  main:
    parent: serve-results
    name: "AWS Glue Data Catalog"
    weight: 25
---

The [AWS Glue Data
Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
is a central metadata repository for data in your AWS account. Just as you can
register the tables of an Amazon RDS for PostgreSQL database in the Data
Catalog, you can register the views and materialized views you maintain in
Materialize. Teams and tools can then discover them alongside your other data
sets, look up their schemas, and query their always-fresh results over JDBC.

Materialize is wire-compatible with PostgreSQL, so you register it with the
same building blocks you use for RDS:

1. An AWS Glue **JDBC connection** that stores the Materialize endpoint and
   credentials.
1. An AWS Glue **crawler** that reads the schemas of your views over that
   connection and creates a Data Catalog table for each one.

The Data Catalog stores only metadata. Each catalog table points back to the
view in Materialize through its connection, and every query runs against
Materialize, which serves up-to-date results.

## Prerequisites

- An AWS account with permissions to create AWS Glue databases, connections,
  and crawlers, IAM roles, and AWS Secrets Manager secrets.
- An Amazon VPC with a private subnet that can reach your Materialize endpoint.
  See [Step 3](#step-3-set-up-networking).
- The views or materialized views you want to expose.

## Prepare Materialize

### Step 1. Index the views you want to expose

Queries from JDBC clients are served by a Materialize cluster. To serve them
with low latency, [index](/concepts/indexes/) the views you want to expose in
a dedicated serving cluster, so their results are kept up to date in memory.
For example:

```mzsql
CREATE VIEW order_totals AS
    SELECT customer_id, count(*) AS order_count, sum(amount) AS total_amount
    FROM orders
    GROUP BY customer_id;

CREATE INDEX order_totals_idx
    IN CLUSTER serving
    ON order_totals (customer_id);
```

Materialized views can also be exposed. Index them in the serving cluster too
if clients query them frequently.

Optionally, describe your views and their columns with [`COMMENT
ON`](/sql/comment-on/). The crawler can copy these comments into the Data
Catalog.

```mzsql
COMMENT ON VIEW order_totals IS 'Order count and total spend per customer.';
COMMENT ON COLUMN order_totals.total_amount IS 'Sum of order amounts, in USD.';
```

### Step 2. Create a service account for AWS Glue

Create a dedicated, read-only role for AWS Glue and the JDBC clients that use
the catalog.

{{< tabs >}}
{{< tab "Cloud" >}}

Create a service account named `glue_svc` with the **Organization Member** role.
See [Create service
accounts](/security/cloud/users-service-accounts/create-service-accounts/).
Connect once with the new service account to finish creating it, and keep the
app password. You need it in [Step 4](#step-4-store-the-credentials-in-aws-secrets-manager).

{{< /tab >}}
{{< tab "Self-Managed" >}}

```mzsql
CREATE ROLE glue_svc WITH LOGIN PASSWORD '<password>';
```

{{< /tab >}}
{{< /tabs >}}

Then configure the role:

1. Set the role's default cluster to your serving cluster, so its queries use
   the indexes from [Step 1](#step-1-index-the-views-you-want-to-expose):

    ```mzsql
    ALTER ROLE glue_svc SET cluster = serving;
    ```

1. Grant the role access to the serving cluster and to the views:

    ```mzsql
    GRANT USAGE ON CLUSTER serving TO glue_svc;
    GRANT USAGE ON DATABASE materialize TO glue_svc;
    GRANT USAGE ON SCHEMA materialize.public TO glue_svc;
    GRANT SELECT ON materialize.public.order_totals TO glue_svc;
    ```

    The crawler registers only the objects the role can access, so the
    `SELECT` grants also control which views appear in the Data Catalog.

## Configure AWS

### Step 3. Set up networking

AWS Glue connects to JDBC data stores from [elastic network interfaces in your
VPC](https://docs.aws.amazon.com/glue/latest/dg/connection-JDBC-VPC.html). Pick
a private subnet and a security group for the connection:

- The security group must have a self-referencing inbound rule for all TCP
  ports, as AWS Glue requires.
- The subnet must be able to reach Materialize on port `6875`:

{{< tabs >}}
{{< tab "Cloud" >}}

The network interfaces have no public IP addresses, so route the subnet's
internet-bound traffic through a [NAT
gateway](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway.html).
If you restrict access to Materialize with [network
policies](/sql/create-network-policy/), allow the NAT gateway's Elastic IP
address.

{{< /tab >}}
{{< tab "Self-Managed" >}}

If Materialize runs in the same VPC, or one reachable from it, allow inbound
TCP traffic on port `6875` to Materialize from the connection's security group.

{{< /tab >}}
{{< /tabs >}}

### Step 4. Store the credentials in AWS Secrets Manager

Store the service account's credentials in a Secrets Manager secret. AWS Glue
reads the `username` and `password` keys:

```bash
aws secretsmanager create-secret \
    --name materialize/glue_svc \
    --secret-string '{"username": "glue_svc", "password": "<password>"}'
```

For Materialize Cloud, the password is the service account's app password.

### Step 5. Create the AWS Glue connection

Create a JDBC connection that points at Materialize. Replace `<host>` with your
Materialize host, and fill in the subnet, its availability zone, and the
security group from [Step 3](#step-3-set-up-networking):

```bash
aws glue create-connection --connection-input '{
    "Name": "materialize",
    "ConnectionType": "JDBC",
    "ConnectionProperties": {
        "JDBC_CONNECTION_URL": "jdbc:postgresql://<host>:6875/materialize",
        "SECRET_ID": "materialize/glue_svc",
        "JDBC_ENFORCE_SSL": "true"
    },
    "PhysicalConnectionRequirements": {
        "SubnetId": "<subnet-id>",
        "AvailabilityZone": "<availability-zone>",
        "SecurityGroupIdList": ["<security-group-id>"]
    }
}'
```

Materialize Cloud uses certificates issued by a publicly trusted certificate
authority, so no custom certificate is required. For a self-managed deployment
that uses a private certificate authority, see [AWS Glue connection
properties](https://docs.aws.amazon.com/glue/latest/dg/connection-properties.html)
for how to supply a custom certificate.

To check the connection, open it in the AWS Glue console and choose **Test
connection**.

## Register the views in the Data Catalog

### Step 6. Create a Data Catalog database

Create a Data Catalog database to hold the catalog tables for your views:

```bash
aws glue create-database --database-input '{"Name": "materialize"}'
```

### Step 7. Run a crawler

1. Create an [IAM
   role](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)
   for the crawler. Attach the `AWSGlueServiceRole` managed policy and a policy
   that allows `secretsmanager:GetSecretValue` on the secret from [Step
   4](#step-4-store-the-credentials-in-aws-secrets-manager).

1. Create a crawler that targets the connection. The include path has the form
   `<database>/<schema>/<table>`, where `%` matches all tables. Requesting the
   `RAWTYPES` and `COMMENTS` metadata records each column's Materialize type and
   the comments from [Step 1](#step-1-index-the-views-you-want-to-expose):

    ```bash
    aws glue create-crawler \
        --name materialize-views \
        --role <crawler-role-arn> \
        --database-name materialize \
        --targets '{
            "JdbcTargets": [{
                "ConnectionName": "materialize",
                "Path": "materialize/public/%",
                "EnableAdditionalMetadata": ["RAWTYPES", "COMMENTS"]
            }]
        }' \
        --schedule 'cron(0 * * * ? *)'
    ```

    The schedule re-runs the crawler every hour, so the Data Catalog picks up
    new views and schema changes. To exclude views, add `Exclusions` glob
    patterns to the target.

1. Run the crawler and list the catalog tables it created:

    ```bash
    aws glue start-crawler --name materialize-views

    aws glue get-tables --database-name materialize \
        --query 'TableList[].[Name, StorageDescriptor.Location]'
    ```

    Each catalog table's location is the fully qualified name of the view in
    Materialize, for example `materialize.public.order_totals`.

### Alternative: register views with the AWS Glue API

AWS documents JDBC crawlers in terms of tables. If the crawler doesn't create
catalog tables for your views or materialized views, or you want to update the
Data Catalog from your deployment pipeline rather than on a schedule, create the
catalog tables directly with the AWS Glue API.

The following script reads the views and materialized views in a schema from
Materialize's [`information_schema`](/reference/system-catalog/information_schema/) and creates, updates,
or deletes catalog tables to match. Each catalog table uses the same parameters
a crawler sets on JDBC tables. The script only deletes catalog tables that it
created for the same Materialize schema.

```python
"""Sync Materialize views and materialized views into the AWS Glue Data Catalog."""

import argparse
import os

import boto3
import psycopg2

# Marks the catalog tables this script owns, so it never deletes tables that a
# crawler or another tool created.
MANAGED_BY = "materialize:managed-by"
MANAGED_BY_VALUE = "glue-sync"

HIVE_TYPES = {
    "boolean": "boolean",
    "smallint": "smallint",
    "integer": "int",
    "bigint": "bigint",
    "real": "float",
    "double precision": "double",
    "text": "string",
    "character varying": "string",
    "date": "date",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp",
    "bytea": "binary",
}

COLUMNS_QUERY = """
SELECT c.table_name, c.column_name, c.data_type,
       c.numeric_precision, c.numeric_scale
FROM information_schema.tables t
JOIN information_schema.columns c
  ON c.table_catalog = t.table_catalog
 AND c.table_schema = t.table_schema
 AND c.table_name = t.table_name
WHERE t.table_catalog = %s
  AND t.table_schema = %s
  AND t.table_type IN ('VIEW', 'MATERIALIZED VIEW')
ORDER BY c.table_name, c.ordinal_position
"""


def hive_type(data_type, precision, scale):
    # Hive decimals support at most 38 digits of precision.
    if data_type == "numeric" and precision is not None and precision <= 38:
        return f"decimal({precision},{scale or 0})"
    # Types without a Hive equivalent (for example jsonb, uuid, interval, list,
    # map, and unconstrained numeric) are registered as string. aws:RawType
    # keeps the Materialize type.
    return HIVE_TYPES.get(data_type, "string")


def views_in_schema(conn, database, schema):
    views = {}
    with conn.cursor() as cur:
        cur.execute(COLUMNS_QUERY, (database, schema))
        for name, column, data_type, precision, scale in cur:
            views.setdefault(name, []).append(
                {
                    "Name": column,
                    "Type": hive_type(data_type, precision, scale),
                    "Parameters": {"aws:RawType": data_type},
                }
            )
    return views


def table_input(name, columns, args):
    return {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "postgresql",
            "connectionName": args.glue_connection,
            "typeOfData": "view",
            MANAGED_BY: MANAGED_BY_VALUE,
        },
        "StorageDescriptor": {
            "Columns": columns,
            "Location": f"{args.mz_database}.{args.mz_schema}.{name}",
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mz-host", required=True)
    parser.add_argument("--mz-user", required=True)
    parser.add_argument("--mz-database", default="materialize")
    parser.add_argument("--mz-schema", default="public")
    parser.add_argument("--glue-database", required=True)
    parser.add_argument("--glue-connection", required=True)
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.mz_host,
        port=6875,
        dbname=args.mz_database,
        user=args.mz_user,
        password=os.environ["MZ_PASSWORD"],
        sslmode="require",
    )
    views = views_in_schema(conn, args.mz_database, args.mz_schema)

    glue = boto3.client("glue")
    existing = {}
    for page in glue.get_paginator("get_tables").paginate(
        DatabaseName=args.glue_database
    ):
        for table in page["TableList"]:
            existing[table["Name"]] = table

    for name, columns in views.items():
        table = table_input(name, columns, args)
        if name in existing:
            glue.update_table(DatabaseName=args.glue_database, TableInput=table)
        else:
            glue.create_table(DatabaseName=args.glue_database, TableInput=table)
        print(f"registered {args.mz_database}.{args.mz_schema}.{name}")

    prefix = f"{args.mz_database}.{args.mz_schema}."
    for name, table in existing.items():
        params = table.get("Parameters", {})
        location = table.get("StorageDescriptor", {}).get("Location", "")
        if (
            name not in views
            and params.get(MANAGED_BY) == MANAGED_BY_VALUE
            and location.startswith(prefix)
        ):
            glue.delete_table(DatabaseName=args.glue_database, Name=name)
            print(f"removed {location}")


if __name__ == "__main__":
    main()
```

Run the script with the `glue_svc` credentials and AWS credentials that allow
`glue:GetTables`, `glue:CreateTable`, `glue:UpdateTable`, and
`glue:DeleteTable` on the Data Catalog database:

```bash
MZ_PASSWORD='<password>' python glue_sync.py \
    --mz-host <host> \
    --mz-user glue_svc \
    --glue-database materialize \
    --glue-connection materialize
```

Data Catalog table names are lowercase. If a view's name contains uppercase
characters, its catalog table name differs from the view name, but the
location still holds the view's exact name.

## Query the views over JDBC

A JDBC client can use the Data Catalog to find a view and the connection
details for it, then query Materialize with the [PostgreSQL JDBC
driver](/integrations/client-libraries/java-jdbc/):

1. Read the catalog table to get the connection name (the `connectionName`
   parameter) and the view's fully qualified name (the location).
1. Read the AWS Glue connection to get the JDBC URL and the Secrets Manager
   secret.
1. Read the credentials from the secret and connect.

The client's IAM principal needs `glue:GetTable`, `glue:GetConnection`, and
`secretsmanager:GetSecretValue` on the secret. It also needs a network path to
Materialize, as described in [Step 3](#step-3-set-up-networking).

The following Java example uses the AWS SDK for Java 2.x and the PostgreSQL
JDBC driver:

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

public class QueryFromCatalog {
    public static void main(String[] args) throws Exception {
        String catalogDatabase = "materialize";
        String catalogTable = "order_totals";

        try (GlueClient glue = GlueClient.create();
             SecretsManagerClient secrets = SecretsManagerClient.create()) {

            Table table = glue.getTable(r -> r
                .databaseName(catalogDatabase)
                .name(catalogTable)).table();
            String connectionName = table.parameters().get("connectionName");
            // For example, materialize.public.order_totals.
            String location = table.storageDescriptor().location();

            Map<String, String> connection = glue.getConnection(r -> r
                .name(connectionName)
                .hidePassword(false)).connection().connectionPropertiesAsStrings();
            String jdbcUrl = connection.get("JDBC_CONNECTION_URL");

            String secret = secrets.getSecretValue(r -> r
                .secretId(connection.get("SECRET_ID"))).secretString();
            JsonNode credentials = new ObjectMapper().readTree(secret);

            Properties props = new Properties();
            props.setProperty("user", credentials.get("username").asText());
            props.setProperty("password", credentials.get("password").asText());
            props.setProperty("sslmode", "require");

            String view = Arrays.stream(location.split("\\."))
                .map(part -> "\"" + part.replace("\"", "\"\"") + "\"")
                .collect(Collectors.joining("."));

            try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + view)) {
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
        }
    }
}
```

Because the service account's default cluster is the serving cluster, the
query is served from the index created in [Step
1](#step-1-index-the-views-you-want-to-expose).

## Considerations

### Freshness

The Data Catalog holds only the schemas of your views, not their data. Every
query reads current results from Materialize, so registering a view in the
catalog doesn't introduce any staleness. The catalog's schemas are as current
as the last crawler run or sync.

### Type mapping

The Data Catalog uses Hive-compatible column types. The crawler converts each
Materialize type to the closest Hive type, and the `RAWTYPES` option keeps the
original type in the column's `aws:RawType` parameter. JDBC clients receive
Materialize's types from the driver, regardless of the catalog's types.

### Query engines

Catalog tables backed by a JDBC connection are queried through that
connection. Query engines that read data files from Amazon S3 using the Data
Catalog, such as Amazon Athena without a federated connector, can't query these
tables directly.

## Related pages

- [Indexes](/concepts/indexes/)
- [Java cheatsheet](/integrations/client-libraries/java-jdbc/)
- [Create service accounts](/security/cloud/users-service-accounts/create-service-accounts/)
- [`CREATE NETWORK POLICY`](/sql/create-network-policy/)
- [AWS Glue crawlers](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
- [AWS Glue JDBC connections](https://docs.aws.amazon.com/glue/latest/dg/connection-properties.html)
