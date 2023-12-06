CREATE TABLE IF NOT EXISTS "error_logs"
(
    "customerId"        VARCHAR
    ,"customerName"     VARCHAR
    ,"transactionId"    VARCHAR
    ,"transactionDate"  VARCHAR
    ,"sourceDate"       TIMESTAMP
    ,"merchantId"       INT
    ,"categoryId"       INT
    ,"currency"         VARCHAR
    ,"amount"           VARCHAR
    ,"description"      VARCHAR
    ,"errorReason"      VARCHAR
);
