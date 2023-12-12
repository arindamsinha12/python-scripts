#!/usr/bin/env python

"""check_table_exists_snowflake.py: Detect if a table exists without using information_schema."""

"""
How do we check the existence of a table in a database without using system tables in information_schema?
One option is to do a select from the table within a try except block and analysing any exceptions.
However, a more elegant solution is to try to create the table 'if not exists' with a dummy field name,
and checking if the table got created. Does require CREATE permission in the schema. This example is in
Snowflake but the method will work in any database.
"""

import snowflake.connector
import pandas as pd
import csv

from snowflake.connector.pandas_tools import write_pandas

__author__ = "Arindam Sinha"
__license__ = "GPL"
__version__ = "1.0.0"
__status__ = "Prototype"

# Create snowflake connection
def create_conn():
    contx = snowflake.connector.connect(
          account='<Snowflake account>',
          user='<Username>',
          password='<Password>'
          )

    cursor = contx.cursor()

    return contx, cursor


if __name__ == "__main__":
    curr = ctx = None

    try:
        ctx, curr = create_conn()

        curr.execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse "
                              "WITH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND=60")
        curr.execute("CREATE DATABASE IF NOT EXISTS testdb")
        curr.execute("USE DATABASE testdb")
        curr.execute("CREATE SCHEMA IF NOT EXISTS testschema")

        curr.execute("USE WAREHOUSE tiny_warehouse")
        curr.execute("USE DATABASE testdb")
        curr.execute("USE SCHEMA testdb.testschema")

        # Now try to create the table with a dummy field
        curr.execute("CREATE TABLE IF NOT EXISTS testschema.testtable (dummy_table_exist_check int)")

        # Execute a SELECT to get the field names
        res = curr.execute("SELECT * FROM testschema.testtable LIMIT 1")

        # If the first column name matches the dummy field name, the table didn't exist
        if res.description[0][0] == 'DUMMY_TABLE_EXIST_CHECK':
            table_exists = False
        else:
            table_exists = True

        if table_exists:
            print("Table exists.")
            # Do something
        else:
            print("Table doesn't exist.")

            # Drop the dummy table
            curr.execute("DROP TABLE testschema.testtable")

            # Do something else

        curr.execute("DROP WAREHOUSE IF EXISTS tiny_warehouse")
    except Exception as ex:
        print(f"Failed: {ex}")
    finally:
        curr.close()
        ctx.close()
