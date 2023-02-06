import keyring

# Store the PostgreSQL credentials in the keyring
keyring.set_password("postgres", "username", "postgres")
keyring.set_password("postgres", "host", "localhost")
keyring.set_password("postgres", "database", "postgres")
keyring.set_password("postgres", "password", "postgres")




