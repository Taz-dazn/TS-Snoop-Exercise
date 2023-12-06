# Global Data Exercise

## How to run the Script (READ FULLY BEFORE RUNNING)

#### Prerequisites
1. Make sure your machine runs Python v3.9 (3.6+ should work)
2. Install the latest **pipenv** version
``` shell
pip install pipenv 
```
3. Create a **config.yaml** at the project root to include the following values for PostgresSQL credentials
``` shell
POSTGRES_HOST: '***'
POSTGRES_USER: '***'
POSTGRES_PASSWORD: '***'
POSTGRES_DATABASE: '***'
POSTGRES_PORT: '***'
```

### Run the Script
- Run the following line in your command line from the project root, followed by the batch_date argument:
``` shell
pipenv run python snoop_program.py --file_source=local --file_location=tech_assessment_transactions.json
```
- file_source = source of the file, can only be s3 or local
- file_location = location of the file, local file path or full s3_key

## How to run the Tests (READ FULLY BEFORE RUNNING)
1. Make sure your machine runs Python v3.9 (3.6+ should work)
2. Install the latest **pipenv** version
``` shell
pip install pipenv 
```

### Run the Script
- Run the following line in your command line from the project root, followed by the batch_date argument:
``` shell
pipenv run python -m pytest test/test_snoop_program.py
```
- This will run the tests within the pip environment
