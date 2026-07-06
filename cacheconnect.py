import sys, redis, os, ssl

# Change global connection object to hold the global pool instead
pool = None

DEFAULTS = {
    'host': os.getenv('CACHE_HOST', 'localhost'),
    'port': int(os.getenv('CACHE_PORT', 6379)),
    'username': os.getenv('CACHE_USERNAME', None),
    'password': os.getenv('CACHE_PASSWORD', None),
    'use_tls': os.getenv('CACHE_TLS', 'false').lower() in ('true', '1', 'yes'),
    'ssl_ca_cert': os.getenv('CACHE_SSL_CA_CERT', None),  # None = skip verification
    'ssl_cert_reqs': os.getenv('CACHE_SSL_CERT_REQS', 'none').lower(),  # 'none' → ssl.CERT_NONE
    'socket_timeout': int(os.getenv('SOCKET_TIMEOUT', 25)),
    'socket_connect_timeout': int(os.getenv('SOCKET_CONNECT_TIMEOUT', 15)),
    'retry': os.getenv('RETRY', 'false').lower() in ('true', '1', 'yes'),
    'retry_on_timeout': os.getenv('RETRY_ON_TIMEOUT', 'false').lower() in ('true', '1', 'yes'),
}

def parse_connection_args():
    args = {}
    i = 1
    
    while i < len(sys.argv):
        arg = sys.argv[i]
        
        if arg.startswith('--host'):
            args['host'] = sys.argv[i + 1] if i + 1 < len(sys.argv) else DEFAULTS['host']
            i += 2
        elif arg.startswith('--socket_timeout'):
            try:
                args['socket_timeout'] = int(sys.argv[i + 1]) if i + 1 < len(sys.argv) else DEFAULTS['socket_timeout']
            except ValueError:
                args['socket_timeout'] = DEFAULTS['socket_timeout']
            i += 2
        elif arg.startswith('--socket_connect_timeout'):
            try:
                args['socket_connect_timeout'] = int(sys.argv[i + 1]) if i + 1 < len(sys.argv) else DEFAULTS['socket_connect_timeout']
            except ValueError:
                args['socket_connect_timeout'] = DEFAULTS['socket_connect_timeout']
            i += 2
        elif arg.startswith('--port'):
            try:
                args['port'] = int(sys.argv[i + 1]) if i + 1 < len(sys.argv) else DEFAULTS['port']
            except ValueError:
                args['port'] = DEFAULTS['port']
            i += 2
        elif arg.startswith('--username'):
            args['username'] = sys.argv[i + 1] if i + 1 < len(sys.argv) else DEFAULTS['username']
            i += 2
        elif arg.startswith('--password'):
            args['password'] = sys.argv[i + 1] if i + 1 < len(sys.argv) else DEFAULTS['password']
            i += 2
        elif arg.startswith('--use_tls'):
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                args['use_tls'] = sys.argv[i + 1].lower() in ('true', '1', 'yes')
                i += 2
            else:
                args['use_tls'] = True
                i += 1
        elif arg.startswith('--ssl-ca-cert'):
            args['ssl_ca_cert'] = sys.argv[i + 1] if i + 1 < len(sys.argv) else DEFAULTS['ssl_ca_cert']
            i += 2
        elif arg.startswith('--ssl-cert-reqs'):
            args['ssl_cert_reqs'] = sys.argv[i + 1].lower() if i + 1 < len(sys.argv) else DEFAULTS['ssl_cert_reqs']
            i += 2
        elif arg.startswith('--clear'):
            args['clear_cache'] = True
            i += 1
        else:
            i += 1
    
    return args

def connect_to_datastore():
    """
    Initializes a global thread-safe ConnectionPool once.
    Returns a unique StrictRedis client interface attached to that shared pool.
    """
    global pool
    
    # 1. Parse operational arguments dynamically
    cli_args = parse_connection_args()
    host = cli_args.get('host', DEFAULTS['host'])
    port = cli_args.get('port', DEFAULTS['port'])
    username = cli_args.get('username', DEFAULTS['username'])
    password = cli_args.get('password', DEFAULTS['password'])
    use_tls = cli_args.get('use_tls', DEFAULTS['use_tls'])

    # 2. Lazily spin up the ConnectionPool if it doesn't exist yet
    if pool is None:
        if use_tls:
            print(f"*** Initializing Cache Connection Pool on host {host}:{port} WITH TLS...")
            pool = redis.ConnectionPool(
                connection_class=redis.SSLConnection,  # <-- FIX: Explicitly use SSL connection factory
                host=host,
                port=port,
                username=username,
                password=password,
                decode_responses=True,
                ssl_cert_reqs=ssl.CERT_NONE,
                max_connections=50,
                socket_timeout=25,
                socket_connect_timeout=15
            )
        else:
            print(f"*** Initializing Cache Connection Pool on host {host}:{port} WITHOUT TLS...")
            pool = redis.ConnectionPool(
                host=host,
                port=port,
                username=username,
                password=password,
                decode_responses=True,
                max_connections=50,
                socket_timeout=25,
                socket_connect_timeout=15
            )
    
    # 3. Return a quick, lightweight instance pointing to the thread-pool.
    return redis.StrictRedis(connection_pool=pool)