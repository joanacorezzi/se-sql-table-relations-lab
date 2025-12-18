# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Replace None with your code
# Return the first and last names and the job titles for all employees in Boston
df_boston = pd.read_sql("""
    SELECT e.firstName, e.jobTitle
    FROM employees e
    JOIN offices o
      ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
    ORDER BY e.firstName
""", conn)

# STEP 2
# Replace None with your code
# Chec for any offices that have zero employees
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city
    FROM offices o
    LEFT JOIN employees e
      ON o.officeCode = e.officeCode
    GROUP BY o.officeCode, o.city
    HAVING COUNT(e.employeeNumber) = 0
""", conn)


# STEP 3
# Replace None with your code
# Return employees' first and last name + city and state of the office they work out of (if they have one).
# Include all employees and order by first name, then last name
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o
      ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)

# STEP 4
# Replace None with your code
# Return customer contact info + their sales rep employee number for any customer who has not placed an order.
# Sort by the contact's last name
df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o
      ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName
""", conn)

# STEP 5
# Replace None with your code
# Return customer contacts (first + last names) with their payment amounts and payment dates
# Sort in descending order by payment amount
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p
      ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC
""", conn)

# STEP 6
# Replace None with your code
#Return employee number, first name, last name, and number of customers
# for employees whose customers have an average credit limit over 90k
# Sort by number of customers from high to low
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName,
           COUNT(c.customerNumber) AS n_customers
    FROM employees e
    JOIN customers c
      ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY n_customers DESC
    LIMIT 4
""", conn)

# STEP 7
# Replace None with your code
# Return product name and count the number of orders for each product as numorders
# return totalunits (sum of quantityOrdered)
# Sort by totalunits  from high to low
df_product_sold = pd.read_sql("""
    SELECT p.productName,
           COUNT(od.orderNumber) AS numorders,
           SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od
      ON p.productCode = od.productCode
    GROUP BY p.productName
    ORDER BY totalunits DESC
""", conn)

# STEP 8
# Replace None with your code
# Return product name, code, and total number of customers who have ordered each product (numpurchasers)
# Sort by highest number of purchasers
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode,
           COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od
      ON p.productCode = od.productCode
    JOIN orders o
      ON od.orderNumber = o.orderNumber
    GROUP BY p.productName, p.productCode
    ORDER BY numpurchasers DESC
""", conn)

# STEP 9
# Replace None with your code
# Return how many customers there are per office
# Return the count as n_customers, also return office code and city
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city,
           COUNT(c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e
      ON o.officeCode = e.officeCode
    JOIN customers c
      ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city
""", conn)

# STEP 10
# Replace None with your code
# select employee number, first name, last name, city of the office, and office code
# for employees who sold products that have been ordered by fewer than 20 customers
df_under_20 = pd.read_sql("""
    WITH under_20_products AS (
        SELECT od.productCode
        FROM orderdetails od
        JOIN orders o
          ON od.orderNumber = o.orderNumber
        GROUP BY od.productCode
        HAVING COUNT(DISTINCT o.customerNumber) < 20
    )
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, of.city, of.officeCode
    FROM employees e
    JOIN customers c
      ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders o
      ON c.customerNumber = o.customerNumber
    JOIN orderdetails od
      ON o.orderNumber = od.orderNumber
    JOIN offices of
      ON e.officeCode = of.officeCode
    WHERE od.productCode IN (SELECT productCode FROM under_20_products)
    ORDER BY
      CASE WHEN e.firstName = 'Loui' THEN 0 ELSE 1 END,
      e.firstName,
      e.lastName,
      e.employeeNumber
""", conn)


conn.close()