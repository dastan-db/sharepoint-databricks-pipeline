"""
Test Lakebase service (services/lakebase.py).
Tests PostgreSQL connection management and query execution.

NOTE: All tests skipped - Lakebase (PostgreSQL via Databricks) not used in this deployment.
"""
import pytest
from app.services.lakebase import Lakebase
import time

pytestmark = pytest.mark.skip(reason="Lakebase not used in this deployment")


def test_lakebase_is_singleton():
    """Test that Lakebase is a singleton."""
    from app.services.lakebase import _Lakebase
    instance1 = _Lakebase()
    instance2 = _Lakebase()
    assert instance1 is instance2


def test_lakebase_query_select(lakebase_catalog: str, lakebase_schema: str):
    """Test Lakebase.query() with SELECT statement."""
    query = "SELECT 1 as test_value, 'hello' as test_string"
    rows = Lakebase.query(query)
    
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == 'hello'


def test_lakebase_create_table(lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test Lakebase.query() creates table."""
    cleanup_lakebase_tables("test_lakebase_create")
    
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {lakebase_catalog}.{lakebase_schema}.test_lakebase_create (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )
    """
    rows = Lakebase.query(create_query)
    
    # CREATE TABLE returns empty result
    assert isinstance(rows, list)


def test_lakebase_insert_returning(lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test Lakebase.query() with INSERT ... RETURNING."""
    cleanup_lakebase_tables("test_lakebase_insert")
    
    # Create table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {lakebase_catalog}.{lakebase_schema}.test_lakebase_insert (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )
    """
    Lakebase.query(create_query)
    
    # Insert with RETURNING
    insert_query = f"""
        INSERT INTO {lakebase_catalog}.{lakebase_schema}.test_lakebase_insert (id, name)
        VALUES (1, 'test_name')
        RETURNING *
    """
    rows = Lakebase.query(insert_query)
    
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == 'test_name'


def test_lakebase_update_returning(lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test Lakebase.query() with UPDATE ... RETURNING."""
    cleanup_lakebase_tables("test_lakebase_update")
    
    # Create and populate table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {lakebase_catalog}.{lakebase_schema}.test_lakebase_update (
            id INT PRIMARY KEY,
            value INT
        )
    """
    Lakebase.query(create_query)
    
    insert_query = f"""
        INSERT INTO {lakebase_catalog}.{lakebase_schema}.test_lakebase_update (id, value)
        VALUES (1, 100)
        RETURNING *
    """
    Lakebase.query(insert_query)
    
    # Update with RETURNING
    update_query = f"""
        UPDATE {lakebase_catalog}.{lakebase_schema}.test_lakebase_update
        SET value = 200
        WHERE id = 1
        RETURNING *
    """
    rows = Lakebase.query(update_query)
    
    assert len(rows) == 1
    assert rows[0][1] == 200


def test_lakebase_delete_returning(lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test Lakebase.query() with DELETE ... RETURNING."""
    cleanup_lakebase_tables("test_lakebase_delete")
    
    # Create and populate table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {lakebase_catalog}.{lakebase_schema}.test_lakebase_delete (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )
    """
    Lakebase.query(create_query)
    
    insert_query = f"""
        INSERT INTO {lakebase_catalog}.{lakebase_schema}.test_lakebase_delete (id, name)
        VALUES (1, 'to_delete')
        RETURNING *
    """
    Lakebase.query(insert_query)
    
    # Delete with RETURNING
    delete_query = f"""
        DELETE FROM {lakebase_catalog}.{lakebase_schema}.test_lakebase_delete
        WHERE id = 1
        RETURNING *
    """
    rows = Lakebase.query(delete_query)
    
    assert len(rows) == 1
    assert rows[0][0] == 1


def test_lakebase_connection_persists():
    """Test that Lakebase connection persists across queries."""
    # First query
    query1 = "SELECT 1"
    Lakebase.query(query1)
    
    # Connection should be cached
    assert Lakebase._connection is not None
    connection_time_1 = Lakebase._connection_time
    
    # Second query (should use same connection)
    query2 = "SELECT 2"
    Lakebase.query(query2)
    
    # Connection time should be the same (no refresh)
    assert Lakebase._connection_time == connection_time_1


def test_lakebase_error_handling():
    """Test Lakebase.query() handles invalid SQL gracefully."""
    invalid_query = "INVALID SQL SYNTAX HERE"
    
    with pytest.raises(Exception):
        Lakebase.query(invalid_query)


@pytest.mark.skip(reason="Takes 60+ minutes to test connection refresh")
def test_lakebase_connection_refresh():
    """
    Test that Lakebase refreshes connection after 59 minutes.
    SKIPPED: Would take over an hour to run.
    """
    # Would test _should_refresh_connection() logic
    pass


def test_lakebase_commit_behavior(lakebase_catalog: str, lakebase_schema: str, cleanup_lakebase_tables):
    """Test that Lakebase commits transactions properly."""
    cleanup_lakebase_tables("test_lakebase_commit")
    
    # Create table
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {lakebase_catalog}.{lakebase_schema}.test_lakebase_commit (
            id INT PRIMARY KEY
        )
    """
    Lakebase.query(create_query)
    
    # Insert data
    insert_query = f"""
        INSERT INTO {lakebase_catalog}.{lakebase_schema}.test_lakebase_commit (id)
        VALUES (1)
        RETURNING *
    """
    Lakebase.query(insert_query)
    
    # Verify data persists (indicates commit worked)
    select_query = f"""
        SELECT * FROM {lakebase_catalog}.{lakebase_schema}.test_lakebase_commit
        WHERE id = 1
    """
    rows = Lakebase.query(select_query)
    
    assert len(rows) == 1
    assert rows[0][0] == 1
