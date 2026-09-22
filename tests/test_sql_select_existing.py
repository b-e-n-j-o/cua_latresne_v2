from api.agents.plu_agent.tools.utils.db import sql_select_existing


def test_sql_select_existing_nulls_missing_columns():
    sql = sql_select_existing(
        "p",
        ["gml_id", "libelle", "txt", "typepsc"],
        {"gml_id", "libelle", "typepsc"},
    )
    assert 'p."gml_id"' in sql
    assert 'p."libelle"' in sql
    assert 'p."typepsc"' in sql
    assert 'NULL AS "txt"' in sql
    assert 'p."txt"' not in sql
