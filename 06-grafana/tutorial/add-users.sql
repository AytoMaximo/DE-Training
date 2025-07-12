SELECT currentUser(); -- This SQL command retrieves the current user executing the query

SHOW GRANTS; -- This command lists all privileges granted to the current user

CREATE USER IF NOT EXISTS john_smith IDENTIFIED BY 'password123';
CREATE USER IF NOT EXISTS maria_ivanova IDENTIFIED BY 'secure456';
CREATE USER IF NOT EXISTS li_zhang IDENTIFIED BY 'pass789';
CREATE USER IF NOT EXISTS carlos_garcia IDENTIFIED BY 'mypassword';
CREATE USER IF NOT EXISTS elena_kowalska IDENTIFIED BY 'qwerty321';


GRANT SELECT ON default.* TO john_smith;
GRANT SELECT ON default.* TO maria_ivanova;
GRANT SELECT ON default.* TO li_zhang;
GRANT SELECT ON default.* TO carlos_garcia;
GRANT SELECT ON default.* TO elena_kowalska;

SELECT name FROM system.users;