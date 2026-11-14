# Onboarding a New Entity

1. Add the entity, expected header, primary key, required fields and Silver target data types to `config/entities.yml`. `seed_metadata.py` publishes these to `ENTITY_CONFIG_bronze` and `COLUMN_CONFIG_bronze`.
2. Add the Bronze DDL table.
3. Add validation metadata to `config/validation_rules.yml` when required.
4. Add a dbt Silver model.
5. Add a Gold dimension/fact only when the business needs it.

The parser itself does not change because it detects `HDR|<ENTITY>` / `FTR|<ENTITY>` dynamically and does not depend on source block order.
