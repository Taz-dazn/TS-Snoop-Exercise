import pytest
from moto import mock_s3
from snoop_program import SnoopTransactions
from mock import patch
import boto3

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
    """Full end to end test to show that the loading, transformation and upload works for local file"""
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    snoop_transactions_local = SnoopTransactions('local', 'test/resources/test_data_dq_pass.json')
    snoop_transactions_local.process_file()

    assert snoop_transactions_local.total_transactions_rows == 3
    assert snoop_transactions_local.total_customer_rows == 3


@mock_s3
@patch('psycopg2.connect')
@patch("yaml.load")
def test_end_to_end_s3_dq_pass(mock_yaml_load, mock_connect):
    """Full end to end test to show that the loading, transformation and upload works for s3 file"""
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket='test-bucket')
    s3_client.put_object(
        Body=open('test/resources/test_data_dq_pass.json', 'rb'),
        Bucket='test-bucket',
        Key='path/to/file.json'
    )
    mock_yaml_load.return_value = POSTGRES_CREDENTILS

    snoop_transactions_local = SnoopTransactions('s3', 's3://test-bucket/path/to/file.json')
    snoop_transactions_local.process_file()

    assert snoop_transactions_local.total_transactions_rows == 3
    assert snoop_transactions_local.total_customer_rows == 3


@patch('psycopg2.connect')
@patch("yaml.load")
def test_end_to_end_local_dq_fail(mock_yaml_load, mock_connect):
    """Full end to end test to show that the Exception is raised when the data fails DQ Checks"""
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
    """Raising an Exception when the source is incorrect"""
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = f"Incorrect Source: gcp, it must be one of 'local' or 's3"

    with pytest.raises(Exception, match=error_message):
        snoop_transactions_local = SnoopTransactions('gcp', 'INCORRECT')
        snoop_transactions_local.process_file()


@patch('psycopg2.connect')
@patch("yaml.load")
def test_invalid_file_location_local(mock_yaml_load, mock_connect):
    """Raising an Exception when the local file is invalid"""
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = "Could not find local file: incorrect_file.json"

    with pytest.raises(Exception, match=error_message):
        snoop_transactions_local = SnoopTransactions('local', 'incorrect_file.json')
        snoop_transactions_local.process_file()


@mock_s3
@patch('psycopg2.connect')
@patch("yaml.load")
def test_invalid_file_location_s3(mock_yaml_load, mock_connect):
    """Raising an Exception when the s3 file is invalid"""
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket='test-bucket')
    s3_client.put_object(
        Body=open('test/resources/test_data_dq_pass.json', 'rb'),
        Bucket='test-bucket',
        Key='path/to/file.json'
    )
    mock_yaml_load.return_value = POSTGRES_CREDENTILS
    error_message = 'S3 file not found, Key: s3://test-bucket/path/to/incorrect_file.json'

    with pytest.raises(FileNotFoundError, match=error_message):
        snoop_transactions_local = SnoopTransactions('s3', 's3://test-bucket/path/to/incorrect_file.json')
        snoop_transactions_local.process_file()
