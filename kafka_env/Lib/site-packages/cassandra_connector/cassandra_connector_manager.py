import json
import os
from .cassandra_connector import CassandraConnector

class CassandraConnectorManager:
    """
    Manages connections to Cassandra databases, including Astra DB, by storing
    and utilizing connection parameters defined in environment variables or passed
    dynamically. This manager supports both Cassandra and Astra DB connections,
    initializing them as needed and providing a way to retrieve active connections.
    """
    def __init__(self):
        """
        Initializes the CassandraConnectionsManager instance by loading connection
        parameters from optional environment variables for both Cassandra and Astra DB.
        It sets up the structure for managing these connections.
        """
        self._connection_params = {}
        self._connections = {}

        # Store CASSANDRA connection params if CASSANDRA_CONNECTION env var is set
        cassandra_conn_str = os.getenv("CASSANDRA_CONNECTION")
        if cassandra_conn_str:
            self._connection_params['env_cassandra'] = _parse_connection_args_json(cassandra_conn_str) 

        # Store Astra connection params if Astra env vars are set
        astra_token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        astra_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
        astra_db_id = os.getenv("ASTRA_DB_DATABASE_ID")
        astra_db_region = os.getenv("ASTRA_DB_REGION")
        astra_scb = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH")
        if astra_token:
            self._connection_params['env_astra'] = {'astra': {'token': astra_token, 'endpoint': astra_endpoint, 'datacenterID': astra_db_id, 'regionName': astra_db_region, 'scb': astra_scb}}

    def get_connector(self, db_key='env_astra', **connection_args):
        """
        Retrieves an existing connection object based on the provided `db_key`, or
        initializes a new connection if one does not already exist for the key. This
        method accepts additional connection parameters dynamically, which must follow
        a specific format depending on the type of database (Cassandra or Astra).

        Connections specified in the environment may be accessed with db_key values of 
        `env_astra` or `env_cassandra`. Otherwise, `connection_args` must be specified 
        the first time a `db_key` is used.

        See CassandraConnector class for format of `connection_args`.

        Args:
            db_key (str, optional): The key identifying the database connection to retrieve
                or initialize. Defaults to 'env_astra', which corresponds to an Astra DB
                connection specified in environment variables.
            **connection_args: Arbitrary keyword arguments providing additional connection
                parameters for initializing a new connection if necessary, following the
                specific format requirements for Cassandra or Astra.

        Returns:
            object: An instance of the CassandraConnector corresponding to the specified
                `db_key`, configured according to the provided `connection_args`.

        Raises:
            ValueError: If the `db_key` is not recognized, the connection parameters
                were not provided or did not follow the required format, or if the
                connection could not be initialized due to an error.
        """
        # Return the existing connector if it's already initialized
        if db_key in self._connections:
            return self._connections[db_key]
        
        if db_key not in self._connection_params and connection_args:
            self._connection_params[db_key] = connection_args

        if db_key in self._connection_params:
            try:
                params = self._connection_params[db_key]
                self._connections[db_key] = CassandraConnector(**params)
                print(f"Connection for '{db_key}' initialized successfully.")
            except Exception as e:
                print(f"Failed to setup connection for '{db_key}'")
                print(f"Error: {str(e)}")
                raise ValueError(f"Connection for '{db_key}' could not be initialized.")
            return self._connections[db_key]

        # If the connection key is not recognized or parameters were not provided
        raise ValueError(f"Connection parameters for '{db_key}' not configured.")
    
def _parse_connection_args_json(conn_args_str):
    """
    Parses a connection arguments string in JSON format into a Python dictionary.

    Args:
        conn_args_str (str): Connection arguments as a JSON-formatted string.

    Returns:
        dict: Connection arguments as a dictionary.
    """
    try:
        conn_args_dict = json.loads(conn_args_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing connection arguments: {str(e)}")

    return conn_args_dict
