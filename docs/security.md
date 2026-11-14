# Security Design

## IAM and Service Accounts

Use separate service identities for:

- Cloud Composer orchestration
- Dataflow workers
- Dataproc jobs
- CI/CD deployment
- BI/reporting consumers

Grant only the roles required by each workload. Avoid using owner/editor roles for runtime service accounts.

## Row-Level Security

Regional analyst access can be restricted with BigQuery row access policies. Example SQL is available in:

`bigquery/security_controls.sql`

A regional analyst can be limited to rows where `region = 'UK'` or another approved region.

## Column-Level Security

Sensitive customer attributes can be protected using BigQuery policy tags/data policies. In a production deployment, columns such as body measurements or other sensitive customer-profile attributes should receive appropriate policy tags.

Column-level controls are applied through BigQuery/Data Catalog policy-tag configuration rather than plain table DDL in this repository.

## Authorized Views

Reporting users should normally consume approved reporting views instead of receiving broad direct access to curated datasets.

Recommended pattern:

```text
Curated Dataset
      |
      v
Authorized Reporting View
      |
      v
BI / Analyst Group
```

## Repository Security

- never commit service-account JSON keys
- use GitHub Actions Workload Identity Federation for CI/CD
- store environment-specific values as GitHub repository variables/secrets
- keep local dbt `profiles.yml` outside version control
- use Secret Manager for runtime secrets when required

## Network and Storage

For a production environment, consider:

- private IP Composer/Dataproc where required
- VPC Service Controls for sensitive environments
- uniform bucket-level access
- CMEK where organizational policy requires customer-managed encryption
- bucket lifecycle policies for temporary/staging objects
