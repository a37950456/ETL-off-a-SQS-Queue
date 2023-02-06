# ETL-off-a-SQS-Queue
Your objective is to:
1. read JSON data containing user login behavior from an AWS SQS Queue, that is made
available via a custom localstack image that has the data pre loaded.
2. Fetch wants to hide personal identifiable information (PII). The fields `device_id` and `ip`
should be masked, but in a way where it is easy for data analysts to identify duplicate
values in those fields.
3. Once you have flattened the JSON data object and masked those two fields, write each
record to a Postgres database that is made available via a custom postgres image that has the tables pre created.


## To run the code
1. Clone this repo.
```bash
git clone https://github.com/a37950456/ETL-off-a-SQS-Queue.git
```

2. Go into the cloned repo.
```bash
cd ETL-off-a-SQS-Queue
```

3. Ready with list of  dependencies.
* docker -- docker install guide 
* docker-compose 
* pip install awscli-local
* Psql
* Postgres docker image
* LocalStack docker image

4. Pull and start docker containers.
  - Postgres 
  - LocalStack
  ```bash
  docker compose up
  ```

5. Test if you can read a message form the queue using awslocal.
```bash
awslocal sqs receive-message  --queue-url http://localhost:4566/000000000000/login-queue 
```

6. Test postgres.
```bash
psql -d postgres -U postgres -p 5432 -h localhost -W 
postgres=# select * from user_logins; 
```

7. Run Python code to init Postgres keyring.
```bash
python PostgresSQL_init.py
```

8. Run Python code to perform ETL process.
```bash
python ETL.py --endpoint-url http://localhost:4566 --queue-name login-queue --max-messages 25
```


## Decrypting masked PIIs
- The `ip` and `device_id` fields are masked using pseudonymize, and we saved the mapping table with ip.csv and device.csv.

- To recover the encrypted fields, we can use the function 
reverse_pseudonymize(pseudonymized_value, type) in this code.


## Checked Postgres connections
- If you can't connet to Postgres successfully, make sure you have add postgres to local path

## Checked AWS
- If you can run 'awslocal sqs receive-message  --queue-url http://localhost:4566/000000000000/login-queue 
' successfully, you will get one message like query_sample.json

