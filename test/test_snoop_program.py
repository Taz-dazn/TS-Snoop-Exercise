import pytest
from moto import mock_s3
from snoop_program import SnoopTransactions
from mock import patch

POSTGRES_CREDENTILS = {
    'POSTGRES_HOST': 'hosting',
    'POSTGRES_USER': 'user',
    'POSTGRES_PASSWORD': 'pass',
    'POSTGRES_DATABASE': 'db',
    'POSTGRES_PORT': 999
}


@patch('psycopg2.connect')
@patch("yaml.load")
def test_end_to_end_local_dq_pass(mock_yaml_load, mock_connect):
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    snoop_transactions_local = SnoopTransactions('local', 'test/resources/test_data_dq_pass.json')
    snoop_transactions_local.process_file()

    assert snoop_transactions_local.total_transactions_rows == 3
    assert snoop_transactions_local.total_customer_rows == 3


@patch('psycopg2.connect')
@patch("yaml.load")
def test_end_to_end_local_dq_fail(mock_yaml_load, mock_connect):
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = "DQ Check FAILED!!! Errors"

    with pytest.raises(Exception, match=error_message):
        snoop_transactions_local = SnoopTransactions('local', 'test/resources/test_data_dq_errors.json')
        snoop_transactions_local.process_file()

        assert snoop_transactions_local.total_transactions_rows == 6
        assert snoop_transactions_local.total_customer_rows == 2


@patch('psycopg2.connect')
@patch("yaml.load")
def test_invalid_file_source(mock_yaml_load, mock_connect):
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = f"Incorrect Source: gcp, it must be one of 'local' or 's3"

    with pytest.raises(Exception, match=error_message):
        snoop_transactions_local = SnoopTransactions('gcp', 'INCORRECT')
        snoop_transactions_local.process_file()


@patch('psycopg2.connect')
@patch("yaml.load")
def test_invalid_file_location(mock_yaml_load, mock_connect):
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = "Could not find local file: incorrect_file.json"

    with pytest.raises(Exception, match=error_message):
        snoop_transactions_local = SnoopTransactions('local', 'incorrect_file.json')
        snoop_transactions_local.process_file()


@patch('psycopg2.connect')
@patch("yaml.load")
def test_invalid_file_location(mock_yaml_load, mock_connect):
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = "Could not find local file: incorrect_file.json"

    with pytest.raises(Exception, match=error_message):
        snoop_transactions_local = SnoopTransactions('local', 'incorrect_file.json')
        snoop_transactions_local.process_file()

