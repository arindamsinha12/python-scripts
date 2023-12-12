# This script shows two ways in which we can recursively find a hierarchy using a parent
# child relationship between table columns. The two methods use (a) the START WITH...CONNECT BY
# construct and (b) recursive CTE. Both methods should work in most modern databases
# though the START WITH...CONNECT BY is not supported in all databases.

# Table used for this example (employee_dataset.csv) is in https://github.com/arindamsinha12/scripts/tree/main/data

# 1) Using START WITH...CONNECT BY
SELECT emp.employee_id, emp.employee_name, emp.supervisor_employee_id, sup.employee_name AS supervisor_name
FROM testdb.testschema.employee emp
LEFT JOIN testdb.testschema.employee sup
ON emp.supervisor_employee_id = sup.employee_id
START WITH emp.employee_id = 1
CONNECT BY emp.supervisor_employee_id = prior emp.employee_id;

# 2) Using recursive CTE
WITH RECURSIVE supervisors
          (employee_id, employee_name, supervisor_employee_id, supervisor_name)
    AS
      (
        SELECT emp.employee_id, emp.employee_name, emp.supervisor_employee_id, sup.employee_name AS supervisor_name
        FROM testdb.testschema.employee emp
        LEFT JOIN testdb.testschema.employee sup
        ON emp.supervisor_employee_id = sup.employee_id
        WHERE emp.employee_id = 1

        UNION ALL

        SELECT emp.employee_id, emp.employee_name, emp.supervisor_employee_id, sup.employee_name AS supervisor_name
        FROM testdb.testschema.employee emp
        LEFT JOIN testdb.testschema.employee sup
        ON emp.supervisor_employee_id = sup.employee_id
        JOIN supervisors supv
        ON emp.supervisor_employee_id = supv.employee_id
     )

 SELECT *
   FROM supervisors;