import json
import logging
import sys
import tempfile
from argparse import ArgumentParser
from typing import Tuple, List, Dict
import boto3
import pandas as pd
import psycopg2
import yaml
from botocore.exceptions import ClientError
from yaml.loader import SafeLoader

logging.basicConfig(level=logging.INFO)


class SnoopTransactions:
    """
    Ingest SnoopTransactions, Perform Data Quality Checks and Upload to PostgreSQL DB
    """

    def __init__(self, file_source: str, file_location: str):
        self.file_source = file_source
        self.file_location = file_location
        self.transactions_df = self._create_transactions_df()
        self._create_postgres_tables()

    def _create_transactions_df(self) -> pd.DataFrame:
        """
        Create a DataFrame for the Transactions input file

        :return: Dataframe of in the Transactions daa
        """
        if self.file_source == 'local':
            data = self._extract_local_file()
            print('_create_transactions_df:::', type(data))

        elif self.file_source == 's3':
            data = self._extract_s3_file()

        else:
            raise Exception(f"Incorrect Source: {self.file_source}, it must be one of 'local' or 's3")

        df = pd.json_normalize(data['transactions'],
                               meta=['customerId', 'customerName', 'transactionId', 'transactionDate', 'sourceDate',
                                     'merchantId', 'categoryId', 'currency', 'amount', 'description'])
        df.columns = ['customerId', 'customerName', 'transactionId', 'transactionDate', 'sourceDate', 'merchantId',
                      'categoryId', 'currency', 'amount', 'description']

        return df

    def _extract_local_file(self) -> Dict:
        """
        Extract the contents of a local file

        :return: input file in a Dict format
        """
        try:
            with open(self.file_location) as f:
                return json.load(f)
        except IOError as error:
            print(error)
            raise IOError(f"Could not find local file: {self.file_location}")

    def _extract_s3_file(self) -> Dict:
        """
        Extract the contents of a file from S3

        :return: input file in a Dict format
        """
        logging.info('Downloading file from s3...')
        s3_client = boto3.client("s3")
        source_s3_bucket = self.file_location.split('/')[2]

        with tempfile.TemporaryDirectory() as tmp_directory:
            tmp_file_location = f"{tmp_directory}/tmp_file.json"
            try:
                s3_client.download_file(source_s3_bucket, self.file_location,
                                        tmp_file_location)
            except ClientError as error:
                print(error)
                raise FileNotFoundError(f'S3 file not found, Key: {self.file_location}')

            logging.info('Filed downloaded from s3!!!')
            with open(tmp_file_location) as f:
                return json.load(f)

    def _create_postgres_tables(self) -> None:
        """
        Create the tables in PostgreSQL using .sql files
        """
        logging.info('Creating PostgreSQL tables...')

        # Running the DDL SQL Scripts to create the tables
        sql_files = ['customers', 'error_logs', 'transactions']
        for file in sql_files:
            self._execute_sql(open(f"ddl/{file}.sql", "r").read())
        logging.info('PostgreSQL tables Created!!!')

    def process_file(self) -> None:
        """
        Process the input file, run the DQ Checks, gather the data to insert, gather the error logs and raise any errors
        """
        output_df, errors_df, error_messages = self._run_data_quality_checks()

        self._load_transactions_data(output_df)
        self._load_customers_data(output_df)
        if len(errors_df.index) > 0:
            self._load_error_logs_data(errors_df)

        if error_messages:
            raise Exception(f"DQ Check FAILED!!! Errors: {', '.join(error_messages)}")

    def _run_data_quality_checks(self) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """
        Perform the DQ Checks. Run the de-dup, validate currency column and validate the transactionDate column.
        Generate the cleaned DataFrame and the errors Data Frame

        :return: Tuple of Cleaned DatFrame, Errors DataFrame and Error Messages
        """
        logging.info('Running DQ Checks...')

        # Validating the currency column
        df, errors_invalid_cur_df, errors_invalid_cur_message = self._data_validation_currency(self.transactions_df)

        # Validating the transactionDate column
        df, errors_invalid_trans_date_df, errors_invalid_trans_date_message = self._data_validation_transaction_date(df)

        # De Duplicating the transaction records
        df, errors_dedup_df, errors_dedup_message = self._data_deduplicate_transaction(df)

        # Combine all records that failed the DQ checks
        errors_df = pd.concat([errors_dedup_df, errors_invalid_cur_df, errors_invalid_trans_date_df])

        # Combine all the error messages from the DQ checks
        error_messages = list(filter(None, [errors_dedup_message, errors_invalid_cur_message, errors_invalid_trans_date_message]))

        logging.info('DQ Checks Completed!!!')
        return df, errors_df, error_messages

    def _load_customers_data(self, df) -> None:
        """
        Load Customers data to DB using UPSERT logic

        :param df: Customers DF to insert
        """
        logging.info('Loading customers data...')
        df = df.sort_values('transactionDate').drop_duplicates(['customerId'], keep='last')
        df = df[['customerId', 'transactionDate']]
        df = df.rename(columns={'transactionDate': 'transactionDateLatest'})

        query = f"""
                    INSERT INTO customers("customerId", "transactionDateLatest")
                    VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                    ON CONFLICT ("customerId")
                    DO  UPDATE SET "transactionDateLatest"= EXCLUDED."transactionDateLatest"
                 """

        self._execute_sql(query)
        self.total_customer_rows = len(df.index)
        logging.info(f'Customers data loaded to DB!!!')

    def _load_transactions_data(self, df) -> None:
        """
        Load Transactions data to DB using UPSERT logic

        :param df: Transactions DF to insert
        """
        logging.info('Loading transactions data...')
        df['customerName'] = '******'
        query = f"""
                    INSERT INTO transactions("customerId", "customerName", "transactionId", "transactionDate", 
                    "sourceDate", "merchantId", "categoryId", "currency", "amount", "description")
                    VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                    ON CONFLICT ("customerId", "transactionId")
                    DO  UPDATE SET "customerName"=EXCLUDED."customerName","transactionDate"=EXCLUDED."transactionDate", 
                                    "sourceDate"=EXCLUDED."sourceDate", "merchantId"=EXCLUDED."merchantId", 
                                    "categoryId"=EXCLUDED."categoryId", "currency"=EXCLUDED."currency",
                                    "amount"=EXCLUDED."amount", "description"=EXCLUDED."description"
                 """

        self._execute_sql(query)
        self.total_transactions_rows = len(df.index)
        logging.info(f'Transactions data loaded to DB!!!')

    def _load_error_logs_data(self, df) -> None:
        """
        Load Error Logs data to DB using UPSERT logic

        :param df: Errors DF to insert
        """
        logging.info('Loading error logs data...')
        df['customerName'] = '******'
        query = f"""
                    INSERT INTO error_logs("customerId", "customerName", "transactionId", "transactionDate", 
                    "sourceDate", "merchantId", "categoryId", "currency", "amount", "description", "errorReason")
                    VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                 """

        self._execute_sql(query)
        self.total_error_logs_rows = len(df.index)
        logging.info(f'Error Logs data loaded to DB, Total Rows!!!')

    def _execute_sql(self, sql_query) -> None:
        """
        Execute SQL Query in PostgresSQL

        :param sql_query: SQL Query to execute
        """
        POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE = self._get_db_credentials()

        conn = None
        try:
            conn = psycopg2.connect(
                database=POSTGRES_DATABASE, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST,
                port=POSTGRES_PORT
            )

            with conn.cursor() as cursor:
                cursor.execute(sql_query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _get_db_credentials() -> Tuple[str, str, str, str, str]:
        """
        Get the PostgreSQL DB credentials from config.yaml

        :return: Tuple of all the DB Credentials
        """
        # Open the file and load the file
        with open('config.yaml') as f:
            credentials = yaml.load(f, Loader=SafeLoader)

        # access values from dictionary and set as env vars
        return credentials['POSTGRES_USER'], credentials['POSTGRES_PASSWORD'], credentials['POSTGRES_HOST'], credentials['POSTGRES_PORT'], credentials['POSTGRES_DATABASE']

    @staticmethod
    def _data_deduplicate_transaction(df) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
        """
        Perform De-duplication on the transaction data

        :param df: Transaction DataFrame
        :return: Tuple of Cleaned DatFrame, Errors DataFrame and Error Message
        """
        df_dedup = df.sort_values('sourceDate').drop_duplicates(['customerId', 'transactionId'], keep='last')

        df_errors = pd.concat([df, df_dedup]).drop_duplicates(keep=False)
        df_errors['errorReason'] = 'Duplicate Record'

        return df_dedup, df_errors, 'Duplicate Record(s)' if len(df_errors) > 0 else None

    @staticmethod
    def _data_validation_currency(df) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
        """
        Perform Currency Validation on the transaction data

        :param df: Transaction DataFrame
        :return: Tuple of Cleaned DatFrame, Errors DataFrame and Error Message
        """
        currencies = ['EUR', 'GBP', 'USD']

        df_validation = df.loc[df['currency'].isin(currencies)]

        df_errors = pd.concat([df, df_validation]).drop_duplicates(keep=False)
        df_errors['errorReason'] = 'Invalid Currency'

        return df_validation, df_errors, 'Invalid Currency(s)' if len(df_errors) > 0 else None

    @staticmethod
    def _data_validation_transaction_date(df) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
        """
        Perform TransactionDate Validation on the transaction data

        :param df: Transaction DataFrame
        :return: Tuple of Cleaned DatFrame, Errors DataFrame and Error Message
        """
        df['transactionDateValidate'] = df['transactionDate']
        df['transactionDateValidate'] = pd.to_datetime(df['transactionDateValidate'], format='%Y-%m-%d',
                                                       errors='coerce')
        df_valid_transaction_date = df.dropna(subset=['transactionDateValidate'])

        df_errors = pd.concat([df, df_valid_transaction_date]).drop_duplicates(keep=False)

        df_valid_transaction_date = df_valid_transaction_date.drop(columns=['transactionDateValidate'])
        df_errors = df_errors.drop(columns=['transactionDateValidate'])
        df_errors['errorReason'] = 'Invalid transactionDate'

        return df_valid_transaction_date, df_errors, 'Invalid transactionDate(s)' if len(df_errors) > 0 else None


if __name__ == "__main__":
    parser = ArgumentParser(description="transforming file")
    parser.add_argument(
        "--file_source",
        nargs="?",
        help="Source of the file: local or s3",
    )
    parser.add_argument(
        "--file_location",
        nargs="?",
        help="File path of the location of the file: local path or full s3 key",
    )
    args = parser.parse_args()
    transactions = SnoopTransactions(args.file_source, args.file_location)
    sys.exit(
        transactions.process_file(),
    )
