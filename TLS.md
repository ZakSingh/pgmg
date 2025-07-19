# TLS Support in pgmg

pgmg supports TLS/SSL connections to PostgreSQL databases through the `tls` feature flag.

## Building with TLS Support

TLS support is not included by default. To enable it:

```bash
# Install from source with TLS
cargo install pgmg --features tls

# Or build locally
cargo build --release --features tls
```

## Configuration

TLS can be configured through:

1. **Connection string parameters**
2. **pgmg.toml configuration file**
3. **Environment variables**

### Connection String

Add TLS parameters to your PostgreSQL connection string:

```
postgres://user:pass@host:5432/database?sslmode=require
postgres://user:pass@host:5432/database?sslmode=verify-full&sslrootcert=/path/to/ca.crt
```

### Configuration File (pgmg.toml)

```toml
connection_string = "postgres://localhost/mydb"

[tls]
sslmode = "require"              # disable, prefer, require, verify-ca, verify-full
sslrootcert = "/path/to/ca.crt"  # CA certificate for server verification
sslcert = "/path/to/client.crt"  # Client certificate
sslkey = "/path/to/client.key"   # Client private key
```

### Environment Variables

```bash
export PGSSLMODE=require
export PGSSLROOTCERT=/path/to/ca.crt
export PGSSLCERT=/path/to/client.crt
export PGSSLKEY=/path/to/client.key
```

## SSL Modes

- **disable**: No TLS encryption
- **prefer**: Try TLS first, fall back to unencrypted (requires TLS build)
- **require**: Require TLS encryption (requires TLS build)
- **verify-ca**: Require TLS and verify server certificate against CA (requires TLS build)
- **verify-full**: Require TLS, verify CA, and verify hostname matches certificate (requires TLS build)

## Priority Order

When TLS settings are specified in multiple places, they are applied in this order (highest priority first):

1. Connection string parameters
2. Environment variables
3. pgmg.toml configuration file

## Examples

### AWS RDS

```toml
connection_string = "postgres://username:password@myinstance.region.rds.amazonaws.com:5432/mydb"

[tls]
sslmode = "verify-full"
sslrootcert = "/path/to/rds-ca-2019-root.pem"
```

### Google Cloud SQL

```toml
connection_string = "postgres://username:password@127.0.0.1:5432/mydb"

[tls]
sslmode = "verify-full"
sslrootcert = "/path/to/server-ca.pem"
sslcert = "/path/to/client-cert.pem"
sslkey = "/path/to/client-key.pem"
```

### Local Development

For local development, you might want to disable TLS:

```toml
connection_string = "postgres://localhost/mydb?sslmode=disable"
```

## Troubleshooting

If you get an error about TLS not being supported:
- Ensure pgmg was built with the `tls` feature flag
- Check with: `pgmg --version` (future versions will show enabled features)

For certificate errors:
- Verify certificate file paths are correct
- Ensure certificates are in PEM format
- Check certificate permissions are readable