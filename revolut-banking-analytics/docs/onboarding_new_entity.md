# Onboarding a New Entity

1. Add the entity and expected header to `config/entities.yml`.
2. Add the Bronze DDL table.
3. Add validation metadata to `config/validation_rules.yml` when required.
4. Add a dbt Silver model.
5. Add a Gold dimension/fact only when the business needs it.

The parser itself does not change because it detects `HDR|<ENTITY>` / `FTR|<ENTITY>` dynamically and does not depend on source block order.
