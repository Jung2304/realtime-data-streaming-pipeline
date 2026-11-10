CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS spark_streams.created_users (
  user_id UUID PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  gender TEXT,
  address TEXT,
  postcode TEXT,
  email TEXT,
  username TEXT,
  dob TEXT,
  registered_date TEXT,
  phone TEXT,
  picture TEXT
);