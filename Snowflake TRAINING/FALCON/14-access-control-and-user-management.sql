
-- 14.0.0  Access Control and User Management
--         Expect this lab to take approximately 40 minutes.
--         Lab Purpose: Students will work with the Snowflake security model and
--         learn how to create roles, grant privileges, build, and implement
--         basic security models.

-- 14.1.0  Determine Privileges (GRANTs)

-- 14.1.1  Navigate to [Worksheets] and create a new worksheet named Managing
--         Security.

-- 14.1.2  If you haven’t created the class database or warehouse, do it now

CREATE WAREHOUSE IF NOT EXISTS FALCON_WH;
CREATE DATABASE IF NOT EXISTS FALCON_DB;


-- 14.1.3  Run these commands to see what has been granted to you as a user, and
--         to your roles:

SHOW GRANTS TO USER FALCON;
SHOW GRANTS TO ROLE TRAINING_ROLE;
SHOW GRANTS TO ROLE SYSADMIN;
SHOW GRANTS TO ROLE SECURITYADMIN;

--         NOTE: The TRAINING_ROLE has some specific privileges granted - not
--         all roles in the system would be able to see these results.

-- 14.2.0  Work with Role Permissions

-- 14.2.1  Change your role to SECURITYADMIN:

USE ROLE SECURITYADMIN;


-- 14.2.2  Create two new custom roles, called FALCON_CLASSIFIED and
--         FALCON_GENERAL:

CREATE ROLE FALCON_CLASSIFIED;
CREATE ROLE FALCON_GENERAL;


-- 14.2.3  GRANT both roles to SYSADMIN, and to your user:

GRANT ROLE FALCON_CLASSIFIED, FALCON_GENERAL TO ROLE SYSADMIN;
GRANT ROLE FALCON_CLASSIFIED, FALCON_GENERAL TO USER FALCON;


-- 14.2.4  Change to the role SYSADMIN, so you can assign permissions to the
--         roles you created:

USE ROLE SYSADMIN;


-- 14.2.5  Create a warehouse named FALCON_SHARED_WH:

CREATE WAREHOUSE FALCON_SHARED_WH;


-- 14.2.6  Grant both new roles privileges to use the shared warehouse:

GRANT USAGE ON WAREHOUSE FALCON_SHARED_WH
  TO ROLE FALCON_CLASSIFIED;
GRANT USAGE ON WAREHOUSE FALCON_SHARED_WH
  TO ROLE FALCON_GENERAL;


-- 14.2.7  Create a database called FALCON_CLASSIFIED_DB:

CREATE DATABASE FALCON_CLASSIFIED_DB;


-- 14.2.8  Grant the role FALCON_CLASSIFIED all necessary privileges to create
--         tables on any schema in FALCON_CLASSIFIED_DB:

GRANT USAGE ON DATABASE FALCON_CLASSIFIED_DB
TO ROLE FALCON_CLASSIFIED;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FALCON_CLASSIFIED_DB
TO ROLE FALCON_CLASSIFIED;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE FALCON_CLASSIFIED_DB
TO ROLE FALCON_CLASSIFIED;


-- 14.2.9  Use the role FALCON_CLASSIFIED, and create a table called
--         SUPER_SECRET_TBL inside the FALCON_CLASSIFIED_DB.PUBLIC schema:

USE ROLE FALCON_CLASSIFIED;
USE FALCON_CLASSIFIED_DB.PUBLIC;
CREATE TABLE SUPER_SECRET_TBL (id INT);


-- 14.2.10 Insert some data into the table:

INSERT INTO SUPER_SECRET_TBL VALUES (1), (10), (30);


-- 14.2.11 Assign GRANT SELECT privileges on SUPER_SECRET_TBL to the role
--         FALCON_GENERAL:

GRANT SELECT ON SUPER_SECRET_TBL TO ROLE FALCON_GENERAL;


-- 14.2.12 Use the role FALCON_GENERAL to SELECT * from the table
--         SUPER_SECRET_TBL:

USE ROLE FALCON_GENERAL;
SELECT * FROM FALCON_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;

--         What happens? Why?

-- 14.2.13 Grant role FALCON_GENERAL usage on all schemas in
--         FALCON_CLASSIFIED_DB:

USE ROLE SYSADMIN;
GRANT USAGE ON DATABASE FALCON_CLASSIFIED_DB TO ROLE FALCON_GENERAL;
GRANT USAGE ON ALL SCHEMAs IN DATABASE FALCON_CLASSIFIED_DB TO ROLE FALCON_GENERAL;


-- 14.2.14 Now try again:

USE ROLE FALCON_GENERAL;
SELECT * FROM FALCON_CLASSIFIED_DB.PUBLIC.SUPER_SECRET_TBL;


-- 14.2.15 Drop the database FALCON_CLASSIFIED_DB:

USE ROLE SYSADMIN;
DROP DATABASE FALCON_CLASSIFIED_DB;


-- 14.2.16 Drop the roles FALCON_CLASSIFIED and FALCON_GENERAL:

USE ROLE SECURITYADMIN;
DROP ROLE FALCON_CLASSIFIED;
DROP ROLE FALCON_GENERAL;

--         HINT: What role do you need to use to do this?

-- 14.3.0  Create Parent and Child Roles

-- 14.3.1  Change your role to SECURITYADMIN:

USE ROLE SECURITYADMIN;


-- 14.3.2  Create a parent and child role, and GRANT the roles to the role
--         SYSADMIN. At this point, the roles are peers (neither one is below
--         the other in the hierarchy):

CREATE ROLE FALCON_child;
CREATE ROLE FALCON_parent;
GRANT ROLE FALCON_child, FALCON_parent TO ROLE SYSADMIN;


-- 14.3.3  Give your user name privileges to use the roles:

GRANT ROLE FALCON_child, FALCON_parent TO USER FALCON;


-- 14.3.4  Change your role to SYSADMIN:

USE ROLE SYSADMIN;


-- 14.3.5  Grant the following object permissions to the child role:

GRANT USAGE ON WAREHOUSE FALCON_WH TO ROLE FALCON_child;
GRANT USAGE ON DATABASE FALCON_DB TO ROLE FALCON_child;
GRANT USAGE ON SCHEMA FALCON_DB.PUBLIC TO ROLE FALCON_child;
GRANT CREATE TABLE ON SCHEMA FALCON_DB.PUBLIC
   TO ROLE FALCON_child;


-- 14.3.6  Use the child role to create a table:

USE ROLE FALCON_child;
USE WAREHOUSE FALCON_WH;
USE DATABASE FALCON_DB;
USE SCHEMA FALCON_DB.PUBLIC;
CREATE TABLE genealogy (name STRING, age INTEGER, mother STRING,
   father STRING);


-- 14.3.7  Verify that you can see the table:

SHOW TABLES LIKE '%genealogy%';


-- 14.3.8  Use the parent role and view the table:

USE ROLE FALCON_parent;
SHOW TABLES LIKE '%genealogy%';

--         You will not see the table, because the parent role has not been
--         granted access.

-- 14.3.9  Change back to the SECURITYADMIN role and change the hierarchy so the
--         child role is beneath the parent role:

USE ROLE SECURITYADMIN;
GRANT ROLE FALCON_child to ROLE FALCON_parent;


-- 14.3.10 Use the parent role, and verify the parent can now see the table
--         created by the child:

USE ROLE FALCON_parent;
SHOW TABLES LIKE '%genealogy%';


-- 14.3.11 Suspend and resize the warehouse

USE ROLE TRAINING_ROLE;
ALTER WAREHOUSE FALCON_WH SET WAREHOUSE_SIZE=XSmall;
ALTER WAREHOUSE FALCON_WH SUSPEND;

