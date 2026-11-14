from pathlib import Path
from banking_pipeline.parser import parse_interleaved_text

ROOT=Path(__file__).resolve().parents[1]

def test_day0_has_all_10_entities():
    b=parse_interleaved_text((ROOT/'data/EvalDB_Day0_Interleaved.txt').read_text())
    assert set(b)=={'BRANCHES','CUSTOMERS','EMPLOYEES','ACCOUNTTYPES','ACCOUNTS','CARDS','TRANSACTIONTYPES','TRANSACTIONS','LOANS','LOANPAYMENTS'}

def test_day1_only_customer_account_and_required_new_ids():
    b=parse_interleaved_text((ROOT/'data/EvalDB_Day1_Interleaved.txt').read_text())
    assert set(b)=={'CUSTOMERS','ACCOUNTS'}
    assert {'369','370'}.issubset({r[0] for r in b['CUSTOMERS'].rows})
    assert {'481','482'}.issubset({r[0] for r in b['ACCOUNTS'].rows})

def test_day2_only_customer_account_and_required_new_ids():
    b=parse_interleaved_text((ROOT/'data/EvalDB_Day2_Interleaved.txt').read_text())
    assert set(b)=={'CUSTOMERS','ACCOUNTS'}
    assert {'371','372'}.issubset({r[0] for r in b['CUSTOMERS'].rows})
    assert {'483','484'}.issubset({r[0] for r in b['ACCOUNTS'].rows})


def test_day0_dormant_reactivation_has_no_prior_account17_activity():
    b=parse_interleaved_text((ROOT/'data/EvalDB_Day0_Interleaved.txt').read_text())
    rows=[r for r in b['TRANSACTIONS'].rows if r[1]=='17']
    assert {r[0] for r in rows} == {'1101','1102','1103'}

def test_entity_metadata_contains_required_fields_and_types():
    import yaml
    payload=yaml.safe_load((ROOT/'config/entities.yml').read_text())
    for entity,cfg in payload['entities'].items():
        assert cfg['primary_key'] in cfg['columns']
        assert cfg['required_fields']
        assert set(cfg['columns']) == set(cfg['data_types'])
