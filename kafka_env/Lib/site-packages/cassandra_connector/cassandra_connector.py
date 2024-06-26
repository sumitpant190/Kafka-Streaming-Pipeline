from cassandra.cluster import Cluster
import os
import requests
from tempfile import gettempdir
import time
from urllib.parse import urlparse

class CassandraConnector:
    """
    A connector for establishing connections with Cassandra or Astra databases. This class
    handles the initialization of connections by dynamically loading configuration and
    authentication details, catering to both traditional Cassandra deployments and
    DataStax Astra's cloud database service.

    The connector decides the type of connection (Cassandra or Astra) based on the presence
    of 'astra' key in the connection arguments. For Astra connections, it handles the
    download or retrieval of the secure connection bundle necessary for the connection.

    Attributes:
        cluster (Cluster): The Cassandra cluster object, initialized upon a successful connection.
        session (Session): The session object for executing queries on the Cassandra cluster.
    """
    def __init__(self, **connection_args):
        """
        Initializes a new instance of the CassandraConnector class.

        The method determines the connection type based on the provided arguments
        and sets up the connection accordingly.

        For Cassandra connections, `connection_args` should include:
          - "authProviderClass": The authentication provider class from the DataStax Python driver
            (e.g., "cassandra.auth.PlainTextAuthProvider"). This must be an importable path.
          - "authProviderArgs": A dictionary of arguments for the authentication provider class
            (e.g., {"username": "cassandra", "password": "cassandra"}).
          - Additional connection options such as "contact_points" and "port" that you would use
            on the Cluster object in the DataStax Python driver.

        For Astra connections, `connection_args` should be a nested dictionary with the key 'astra',
        containing:
          - 'token': The application token for accessing your Astra database.
          - 'endpoint': The API endpoint for your Astra database.

        For Astra backwards compatbility with connection mechanics such as CassIO, the 'endpoint' may be 
        omitted from `connection_args`, and the following provided:
          - 'scb': Filesystem path to secure connect bundle
          or
          - 'datacenterID': DB ID of the database
          - 'regionName': Astra region entry point (optional, will default to primary region)

        Example for Cassandra:
            CassandraConnector(authProviderClass='cassandra.auth.PlainTextAuthProvider',
                          authProviderArgs={'username': 'cassandra', 'password': 'cassandra'},
                          contact_points=['127.0.0.1'], port=9042)

        Example for Astra:
            CassandraConnector(astra={'endpoint': 'https://your-astra-db-endpoint',
                                      'token': 'your-astra-db-token'})
        Args:
            **connection_args: Arbitrary keyword arguments for connection parameters.
                See above explanation.
        """
        self._connection_args = connection_args
        self._cluster = None
        self._session = None
        if self._connection_args.get('astra'):
            self._connectionType = 'astra'
            self._setup_astra_connection()
        else:
            self._connectionType = 'cassandra'
            self._setup_cassandra_connection()

    @property
    def session(self, new=False, replace=False):
        """
        Provides access to the Cassandra session object for executing queries. This property allows
        for the retrieval of the current session, the creation of a new session instance without
        affecting the current session, or the replacement of the current session with a new one.

        Args:
            new (bool): If True, creates and returns a new session instance without replacing the
                current session maintained by the class instance. This allows for temporary sessions
                that do not interfere with the existing session's state. Defaults to False, which
                results in returning the existing session.

            replace (bool): If True, shuts down the current session and replaces it with a new one.
                This new session becomes the session returned by subsequent calls to this property.
                If `new` is also True, `new` takes precedence, and a new session is created and
                returned without replacing the current session. Defaults to False.

        Returns:
            cassandra.cluster.Session: The session object for interacting with the Cassandra cluster.
                Depending on the parameters, this could be the existing session, a new session that
                replaces the existing one, or a new temporary session.

        Note:
            Using the `replace` option will shut down the existing session and create a new one. It's
            important to ensure that no operations are pending or currently using the old session
            before replacing it, as this could lead to interrupted operations or resource leaks.
        """
        if replace:
            self._session.shutdown()
            self._session = self._cluster.connect()
            return self._session
        if new:
            return self._cluster.connect()
        return self._session

    @property
    def cluster(self):
        """
        Provides access to the cluster object associated with this connector.

        Returns:
            Cluster: The Cassandra cluster object.
        """
        return self._cluster

    def _setup_cassandra_connection(self):
        """
        Sets up a connection to a Cassandra cluster using the provided connection arguments.

        This method dynamically loads the authentication provider class if specified,
        and initializes the Cluster and Session objects for Cassandra operations.
        """
        auth_provider_class = self._connection_args.pop('authProviderClass', None)
        auth_provider_args = self._connection_args.pop('authProviderArgs', {})

        if auth_provider_class:
            # Dynamically import the auth provider class
            module_path, class_name = auth_provider_class.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            auth_provider_class = getattr(module, class_name)
            auth_provider = auth_provider_class(**auth_provider_args)
        else:
            auth_provider = None

        # Pass the remaining _connection_args and the auth_provider to the Cluster constructor
        self._cluster = Cluster(auth_provider=auth_provider, **self._connection_args)
        self._session = self._cluster.connect()

    def _setup_astra_connection(self):
        """
        Sets up a connection to a DataStax Astra database.

        This method retrieves or downloads the secure connect bundle necessary for the
        connection and then calls `_setup_cassandra_connection` to initialize the Cluster
        and Session objects with Astra-specific parameters.
        """
        astra_args = self._connection_args['astra']
        scb_path = self._get_or_download_secure_connect_bundle(astra_args)        
        self._connection_args = {
            "cloud": {'secure_connect_bundle': scb_path}, 
            "authProviderClass": "cassandra.auth.PlainTextAuthProvider",
            "authProviderArgs": {"username": "token", "password": astra_args['token']},
        }
        self._setup_cassandra_connection() 
    
    def _get_or_download_secure_connect_bundle(self, astra_args):
        """
        Retrieves or downloads the secure connect bundle for Astra connections.

        Args:
            astra_args (dict): A dictionary containing 'endpoint' and 'token' for the
                Astra database, and optionally 'datacenterID' and 'regionName'. If the
                dictionary contains 'scb', that value is returned.

        Returns:
            str: The file path to the secure connect bundle.
        """
        if 'scb' in astra_args and astra_args['scb']:
            return astra_args['scb']

        # Ensure datacenterID and regionName are extracted from endpoint if provided
        if 'endpoint' in astra_args and astra_args['endpoint']:
            # Parse the endpoint URL
            endpoint_parsed = urlparse(astra_args['endpoint'])
            # Extract the hostname without the domain suffix
            hostname_without_suffix = endpoint_parsed.netloc.split('.apps.astra.datastax.com')[0]
            # Split the hostname to get parts
            parts = hostname_without_suffix.split('-')
            # Datacenter is first 5 parts, everything after is region
            datacenterID = '-'.join(parts[:5])
            regionName = '-'.join(parts[5:])

            # Update astra_args with extracted values if not explicitly provided
            astra_args['datacenterID'] = astra_args.get('datacenterID') or datacenterID
            astra_args['regionName'] = astra_args.get('regionName') or regionName
        elif 'datacenterID' not in astra_args or not astra_args['datacenterID']:
            raise ValueError("Astra endpoint or datacenterID must be provided in args.")

        scb_dir = os.path.join(gettempdir(), "cassandra-astra")
        os.makedirs(scb_dir, exist_ok=True)

        # Generate the secure connect bundle filename
        scb_filename = f"astra-secure-connect-{astra_args['datacenterID']}"
        if 'regionName' in astra_args and astra_args['regionName']:
            scb_filename += f"-{astra_args['regionName']}"
        scb_filename += ".zip"
        scb_path = os.path.join(scb_dir, scb_filename)

        if not os.path.exists(scb_path) or time.time() - os.path.getmtime(scb_path) > 360 * 24 * 60 * 60:
            download_url = self._get_secure_connect_bundle_url(astra_args)
            response = requests.get(download_url)
            response.raise_for_status()

            with open(scb_path, 'wb') as f:
                f.write(response.content)
        
        return scb_path

    def _get_secure_connect_bundle_url(self, astra_args):
        """
        Generates the URL for downloading the secure connect bundle for Astra connections.

        Args:
            astra_args (dict): A dictionary containing 'endpoint', 'token', and
                'datacenterID' for the Astra database. Optionally, 'regionName' can be
                provided to download a region-specific secure connect bundle.

        Returns:
            str: The URL to download the secure connect bundle from.
        """
        url_template = astra_args.get(
            'bundleUrlTemplate',
            "https://api.astra.datastax.com/v2/databases/{database_id}/secureBundleURL?all=true"
        )
        url = url_template.replace("{database_id}", astra_args['datacenterID'])

        headers = {
            'Authorization': f"Bearer {astra_args['token']}",
            'Content-Type': 'application/json',
        }
        response = requests.post(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        if not data or len(data) == 0:
            raise ValueError("Failed to get secure bundle URLs.")

        # Default to the first URL if no regionName is specified or if a specific region's bundle cannot be found.
        download_url = data[0]['downloadURL']

        # If 'regionName' is provided, try to find a region-specific bundle.
        if 'regionName' in astra_args and astra_args['regionName']:
            regional_bundle = next((bundle for bundle in data if bundle['region'] == astra_args['regionName']), None)
            if regional_bundle:
                download_url = regional_bundle['downloadURL']
            else:
                raise ValueError(f"Specific bundle for region '{astra_args['regionName']}' not found.")

        return download_url
