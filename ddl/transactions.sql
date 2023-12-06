CREATE TABLE IF NOT EXISTS "transactions"
(
    "customerId"        VARCHAR
    ,"customerName"     VARCHAR
    ,"transactionId"    VARCHAR
    ,"transactionDate"  DATE
    ,"sourceDate"       TIMESTAMP
    ,"merchantId"       INT
    ,"categoryId"       INT
    ,"currency"         VARCHAR
    ,"amount"           VARCHAR
    ,"description"      VARCHAR
    ,PRIMARY KEY("customerId", "transactionId")
);
