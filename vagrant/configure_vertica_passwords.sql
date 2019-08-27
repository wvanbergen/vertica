select SET_CONFIG_PARAMETER('SecurityAlgorithm', 'SHA512');
CREATE AUTHENTICATION default_network METHOD 'hash' HOST '0.0.0.0/0';
CREATE AUTHENTICATION default_local METHOD 'hash' LOCAL;
GRANT AUTHENTICATION default_network to public;
GRANT AUTHENTICATION default_local to public;
