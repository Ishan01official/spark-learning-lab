# Unity Catalog And Security

## Simple Explanation

Unity Catalog is Databricks governance for catalogs, schemas, tables, volumes, permissions, and lineage.

## Production Explanation

Governance should be designed around environments, data sensitivity, and ownership. Bronze data may be restricted to engineers, Silver to domain teams, and Gold to analysts or applications.

## Key Concepts

- Metastore.
- Catalog.
- Schema.
- Table.
- Volume.
- External location.
- Grants.
- Lineage.
- Access mode.

## Common Mistakes

- Using unqualified table names.
- Granting broad workspace admin access instead of table-level permissions.
- Forgetting service principal permissions.
- Mixing legacy Hive metastore assumptions with Unity Catalog behavior.

## Interview Answer

"I would model environments and domains with catalogs and schemas, use least-privilege grants, manage external locations carefully, and run production jobs as service principals with audited permissions."
