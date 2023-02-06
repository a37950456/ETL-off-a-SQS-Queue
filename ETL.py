
# Extract, Transform, Load (ETL) is a process of moving data from one source to another, where it is transformed and loaded into a target system. Here's an example of how you can perform ETL off an Amazon Simple Queue Service (SQS) queue using
import hashlib
import sys
import boto3
import psycopg2
import json
import keyring
from datetime import datetime
import configparser
import hashlib
import argparse
import csv
import os

class ETL_Process():
    """Class for performing ETL Process"""

    def __init__(self, endpoint_url, queue_name, wait_time, max_messages):
        """Constructor to get Postgres credentials"""
 
        self.__username = keyring.get_password("postgres", "username")
        self.__password = keyring.get_password("postgres", "password")
        self.__host = keyring.get_password("postgres", "host")
        self.__database = keyring.get_password("postgres", "database")
        
        # Get argument values
        self.__endpoint_url = endpoint_url
        self.__queue_name = queue_name
        self.__wait_time = wait_time
        self.__max_messages = max_messages
        
        
        # Init mapping file for encode and decode
        self.__ip_mapping = self.mapping("ip.csv")
        self.__device_mapping = self.mapping("device.csv")
        

        # Return from the constructor
        return

    def mapping(self, filename):
        """ Get filename Map """
        mapping = {}
        if not os.path.exists(filename):
            with open(filename, "w") as file:
                file.write("Origin,Transformed\n")
            print(f"The file '{filename}' has been created.")
        else:
            with open(filename, 'r') as file:
                csvreader = csv.reader(file)
                for row in csvreader:
                    k = row[0]
                    v = row[1]
                    mapping[k] = v
        return mapping
        
    def w_mapping(self, filename):
        """ Write new values to filename Map """
        map = {}
        if filename == "ip.csv":
            map = self.__ip_mapping
        elif filename == "device.csv":
            map = self.__device_mapping
        w = csv.writer(open(filename, "w"))
        for key, val in map.items():
            # write every key and value to file
            w.writerow([key, val])
    
    def pseudonymize(self, original_value, type, salt="fetch_reward"):
        """ Encode with pseudonymize and salt  """
        map = {}
        if type == 'ip':
            map = self.__ip_mapping
        elif type == 'device':
            map = self.__device_mapping
        salted_value = original_value + salt
        hashed_value = hashlib.sha256(salted_value.encode()).hexdigest()
        if original_value not in map:
            map[original_value] = hashed_value
        return hashed_value

    
    def reverse_pseudonymize(self, pseudonymized_value, type, salt="fetch_reward"):
        """ Decode with pseudonymize and salt with map  """
        if type == 'ip':
            map = self.__ip_mapping
        elif type == 'device':
            map = self.__device_mapping
        original_values = {value: key for key, value in map.items()}
        if pseudonymized_value in original_values:
            return original_values[pseudonymized_value]
        return None

    def get_messages(self):
        """Function to receive messages from SQS Queue"""

        # Instantiate SQS Client
        sqs_client = boto3.client("sqs", endpoint_url = self.__endpoint_url, region_name='us-east-1',
                                 aws_access_key_id='data_engineering_id_key', aws_secret_access_key='data_engineering_secret_key')

        # Receive messages from queue
        try:
            response = sqs_client.receive_message(
                QueueUrl = self.__endpoint_url + '/' + self.__queue_name,
                MaxNumberOfMessages = self.__max_messages,
                WaitTimeSeconds = self.__wait_time
            )
        except Exception as exceptions:
            # Print error while parsing parameters
            print("Error - " + str(exceptions))

            # Exit from program
            sys.exit()

        # Get messages from SQS
        messages = response['Messages']
        
        # Return the messages
        return messages

    def transform_data(self, messages):
        """transform PII data"""
        message_list = []

        try:
            if messages == None:
                raise IndexError("Message list is EMPTY")
        except IndexError as index_e:
            print(f"Error receiving message: {index_e}")

            sys.exit()

        message_count = 0

        for message in messages:
            message_count += 1
            message_body = json.loads(message['Body'])
                
            try:
                ip = message_body['ip']

            except Exception as e:
                print("Error Message: index " + str(message_count) + "ip is invalid")
                continue

            try:
                device_id = message_body['device_id']

            except Exception as e:
                print("Error Message: index " + str(message_count) + "device_id is invalid -")
                continue

            # Encode ip and device_id
            new_ip = self.pseudonymize(ip, 'ip')
            new_device_id = self.pseudonymize(device_id, 'device')

            message_body['ip'] = new_ip
            message_body['device_id'] = new_device_id

            message_list.append(message_body)
                        
            # load csv module
            self.w_mapping("ip.csv")
            self.w_mapping("device.csv")
            
            return message_list
        
    def load_data_postgre(self, message_list):
        """Function to load data to postgres"""

        # Check if "message_list" is empty
        try:
            if len(message_list) == 0:
                # Raise Type Error
                raise TypeError
        except TypeError as type_error:
            # Print the "message_list" is empty
            print("Error - " + str(type_error))

            # Exit from program
            sys.exit()
            
        postgres_conn = psycopg2.connect(
            host = self.__host,
            database = self.__database,
            user = self.__username,
            password = self.__password
        )
        
        # Create a Cursos
        cursor = postgres_conn.cursor()
        
        # Iterate messages
        for message_json in message_list:
            if message_json['locale'] == None:
                message_json['local'] = 'NA'
            # transform app verstion to int for table init data type
            message_json['app_version'] = int(''.join(x for x in message_json['app_version'].split('.')))
            # add date of time
            message_json['create_date'] = datetime.now().strftime("%Y-%m-%d")
            
            values = list(message_json.values())

            # Execute the insert query
            cursor.execute("""INSERT INTO user_logins ( \
                user_id, \
                app_version, \
                device_type, \
                masked_ip, \
                locale, \
                masked_device_id, \
                create_date \
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)""", values)

            # Commit data to Postgres
            postgres_conn.commit()

        # Close connection to Postgres
        postgres_conn.close()

        # Return from the function
        return
            
def main():
    """ Main function"""
    # Instantiate the argparser
    parser = argparse.ArgumentParser(
        prog = "Extract Transform Load - Process",
        description = "Program extracts data from SQS queue - \
                       Transforms PIIs in the data - \
                       Loads the processed data into Postgres",
        epilog = "Please raise an issue for code modifications"
    )

    # Add arguments
    
    parser.add_argument('-e', '--endpoint-url', required = True ,help = "Pass the endpoint URL here")
    parser.add_argument('-q', '--queue-name', required = True ,help = "Pass the queue URL here")
    parser.add_argument('-t', '--wait-time', type = int, default = 10, help = "Pass the wait time here")
    parser.add_argument('-m', '--max-messages', type = int, default = 10, help = "Pass the max messages to be pulled from SQS queue here")

    # Parse the arguments
    args = vars(parser.parse_args())

    # Get value for each argument
    endpoint_url = args['endpoint_url']
    queue_name = args['queue_name']
    wait_time = args['wait_time']
    max_messages = args['max_messages']

    etl_process_object = ETL_Process(endpoint_url, queue_name, wait_time, max_messages)

    # Extract messages from SQS Queue
    print("Fetching messages from SQS Queue...")
    messages = etl_process_object.get_messages()

    # Transform IIPs from the messages
    print("Masking PIIs from the messages...")
    message_list = etl_process_object.transform_data(messages)
    
    # Load data to Postgres
    print("Loading messages to Postgres...")
    etl_process_object.load_data_postgre(message_list)

    # Return from the main function
    return
    


# Calling the main function
if __name__ == "__main__":
    main()
