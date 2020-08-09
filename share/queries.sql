-- 5a
SELECT c_name AS 'Customer', c_email AS 'Email', c_address AS 'Address', type
    FROM customer_addresses_v;
-- 5b
SELECT c.c_name AS 'Customer', SUM(case when datediff(o.order_date, now()) * -1 < 2 * 365 then ol.QUANTITY * i.price else 0 end) AS 'Total Spent in Past 2 Years'
    FROM orders o 
           NATURAL JOIN orderlines ol 
           NATURAL JOIN menumenuitems i 
           NATURAL JOIN customers c
    GROUP BY c.c_name
    ORDER BY 2 desc
    LIMIT 3;

-- 5c
select e.e_firstname, e.e_lastname, COUNT(*) AS "Total Specialties", GROUP_CONCAT(distinct(m.dishname)) AS "Specialties"
from
    sous s
natural join 
    employees e 
natural join 
    specialties sp 
left outer join
    menuitems m on sp.specialty = m.itemid
group by
    s.employeeid
having
    count(*) >= 3;

-- 5d
SELECT chef1.e_lastname AS 'Chef 1', chef2.e_lastname AS 'Chef 2' FROM
        (SELECT e.e_firstname, e.e_lastname
            FROM employees e
                INNER JOIN exempt ex ON e.employeeid = ex.employeeid
                INNER JOIN cooks c ON ex.employeeid = c.employeeid
                INNER JOIN sous s ON c.employeeid = s.employeeid
                INNER JOIN specialties sp ON s.employeeid = sp.employeeid
                INNER JOIN menuitems mi ON sp.specialty = mi.itemid) chef1
                    
INNER JOIN
        (SELECT e.e_firstname, e.e_lastname
            FROM employees e
                INNER JOIN exempt ex ON e.employeeid = ex.employeeid
                INNER JOIN cooks c ON ex.employeeid = c.employeeid
                INNER JOIN sous s ON c.employeeid = s.employeeid
                INNER JOIN specialties sp ON s.employeeid = sp.employeeid
                INNER JOIN menuitems mi ON sp.specialty = mi.itemid) chef2
                
NATURAL JOIN specialties s1
NATURAL JOIN specialties s2

WHERE chef1.e_firstname > chef2.e_firstname AND chef1.e_lastname <> chef2.e_lastname
GROUP BY chef1.e_lastname, chef2.e_lastname
HAVING COUNT(s1.specialty = s2.specialty) > 2;

-- 5e
SELECT  
    dishname, sum(ORDERLINES.QUANTITY) as 'Units Ordered'
    FROM menuMENUitems 
    NATURAL JOIN MENUITEMS
        NATURAL JOIN orderlines 
        WHERE menutype = 'CHILDRENS'
    group by itemid
    ORDER BY sum(ORDERLINES.QUANTITY) DESC
    LIMIT 3;

 -- 5f
select 
    m.dishname as 'Dish', e.e_shift as 'Shift', concat(e.e_firstname,' ',e.e_lastname) as 'Sous Chef on Duty'
from
    menuitems m 
natural join 
    menumenuitems mm
natural join 
    (orderlines ol left outer join orders o using(order_number))
natural join 
     (employees e inner join shifts sh using(employeeid)) 
where
    e.e_title = 'Sous' and
    e.e_shift = o.order_shift and
    itemid not in (select specialty from employees inner join specialties using(employeeid))
group by itemid;

-- 5g
SELECT 
    C_NAME, type, MIMINGS_MONEY 
FROM 
    customer_addresses_v 
ORDER BY 
    MIMINGS_MONEY DESC;

-- 5h
select
    c.c_name as 'Customer', SUM(ol.QUANTITY * i.price ) as 'Total Spent'
from 
    orders o natural join orderlines ol natural join menumenuitems i natural join customers c
group by 
    c.c_name 
order by
    2 desc;

-- 5i
select
    c.c_name as 'Customer', monthname(o.order_date) as 'Month', count(distinct(order_number)) as 'Number of Visits'
from 
    orders o natural join orderlines ol natural join customers c
group by 
    c.c_name, month(o.order_date)
order by
    3 desc;

-- 5j
select
    c.c_name as 'Customer', SUM(case when datediff(o.order_date, now()) * -1 < 365 then ol.QUANTITY * i.price else 0 end) as 'Total Spent'
from 
    orders o natural join orderlines ol natural join menumenuitems i natural join customers c
group by 
    c.c_name
order by
    2 DESC
LIMIT 3;

-- 5k
SELECT m.dishname, SUM(ol.quantity * mi.price) AS 'Revenue'
    FROM menuitems m
        INNER JOIN menumenuitems mi ON m.itemid = mi.itemid
        INNER JOIN orderlines ol ON mi.priceid = ol.priceid
        INNER JOIN orders o ON ol.order_number = o.order_number
            GROUP BY m.itemid
            ORDER BY SUM(case when datediff(o.order_date, now()) * -1 < 365 then ol.QUANTITY * mi.price else 0 end) DESC
            LIMIT 5;

-- 5L
select
    E.E_FIRSTNAME, E.E_LASTNAME, COUNT(distinct mi.dishname) as 'Number of Mentorships', GROUP_CONCAT(distinct (mi.dishname) order by mi.dishname desc separator ', ') AS "Specialties"
from  
    mentorships m
left outer join
    specialties s on m.specialty = s.specialty 
left outer join 
    menuitems mi on mi.itemid = s.specialty
left outer join
    employees e on e.employeeid = m.mentorid
group by 
    m.mentorid
order by 
    3 desc
limit 1;

-- 5m
select
    mi.dishname, count(*) as 'Number Sous Chefs Specializing'
from
    specialties s
left outer join
    menuitems mi on s.specialty = mi.itemid
group by
    dishname
order by
    2 asc
limit 3;

-- 5n
select
    c.c_name as 'Customer', co.corporation as 'Company', count(*) as 'Count'
from
    customers c
left outer join
    corporations co using(customerid)
group by
    c.c_name
having
    count(*) > 1;

-- 5o
SELECT menutype as Menu, group_concat(dishname, ' @$', price) as 'Contents with Prices' 
    FROM menuitems 
        NATURAL JOIN menumenuitems
        group by menutype
        ORDER BY itemid;

-- 5p
DELIMITER $$
CREATE TRIGGER CheckMaitreD_Insert
    BEFORE INSERT 
    ON SHIFTS FOR EACH ROW
    BEGIN
        DECLARE mCOUNT INT;
        DECLARE nCOUNT INT;
        SET mCOUNT = (SELECT COUNT(*) FROM EMPLOYEES INNER JOIN SHIFTS USING(EMPLOYEEID) 
                    WHERE E_TITLE = 'MaitreD' AND
                        E_SHIFT = 'MORNING' AND
                        SHIFT_DATE = NEW.SHIFT_DATE);
        SET nCOUNT = (SELECT COUNT(*) FROM EMPLOYEES INNER JOIN SHIFTS USING(EMPLOYEEID) 
                    WHERE E_TITLE = 'MaitreD' AND
                        E_SHIFT = 'EVENING' AND
                        SHIFT_DATE = NEW.SHIFT_DATE);
        IF mCOUNT >= 2 OR nCOUNT >= 2
        THEN SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Shift already has a two maitreDs';
        END IF;
    END $$

DELIMITER ;

INSERT IGNORE INTO EMPLOYEES(E_FIRSTNAME, E_LASTNAME, E_DOB, E_ADDRESS, E_PHONE, E_TITLE)
VALUES
    ('TEST1','TESTER','01/06/1959','1 Linguini Court Paris, France','818-325-5563','MaitreD'),
    ('TEST2','TESTER','01/06/1959','1 Linguini Court Paris, France','818-325-5563','MaitreD');
UPDATE EMPLOYEES 
SET E_SHIFT = 
    (CASE
    WHEN E_FIRSTNAME = 'TEST1' or E_FIRSTNAME = 'TEST2' THEN 'EVENING'
    ELSE E_SHIFT
    END);
INSERT IGNORE INTO SHIFTS(EMPLOYEEID, SHIFT_DATE)
VALUES
    (17,'2019-12-05');
INSERT IGNORE INTO SHIFTS(EMPLOYEEID, SHIFT_DATE)
VALUES
    (18,'2019-12-05');
INSERT IGNORE INTO SHIFTS(EMPLOYEEID, SHIFT_DATE)
VALUES
    (18,'2019-12-06');
    
DELIMITER $$
CREATE TRIGGER CheckMimingMoney_Insert
    BEFORE INSERT 
    ON ORDERS FOR EACH ROW
    BEGIN
        IF NOT EXISTS(SELECT * FROM ORDERS 
            WHERE 
                CUSTOMERID = NEW.CUSTOMERID)
        THEN UPDATE CUSTOMERS
            SET CUSTOMERS.MIMINGS_MONEY = CUSTOMERS.MIMINGS_MONEY + 5
            WHERE CUSTOMERS.CUSTOMERID = NEW.CUSTOMERID;
        END IF;
    END $$

DELIMITER ;  

INSERT IGNORE INTO CUSTOMERS (C_NAME,C_EMAIL, C_ADDRESS, MIMINGS_MONEY)
VALUES 
    ('TEST TESTER','TESTTESTER@TEST.com','852 TEST St Long Beach, CA 90755',0);
SELECT C_NAME, MIMINGS_MONEY FROM CUSTOMERS;
INSERT IGNORE INTO ORDERS (CUSTOMERID, ORDER_DATE, TOTAL_PRICE, ORDER_SHIFT)
VALUES
    (1,'2019-07-21',0.0,'MORNING'),
    (6,'2019-11-30',0.0,'MORNING');
SELECT C_NAME, MIMINGS_MONEY FROM CUSTOMERS;

DELIMITER $$
CREATE TRIGGER CheckDishwasher_Insert
	BEFORE INSERT 
    ON SHIFTS FOR EACH ROW
	BEGIN
		IF EXISTS(SELECT * FROM EMPLOYEES INNER JOIN SHIFTS USING(EMPLOYEEID) 
			WHERE 
				E_TITLE = 'DISHWASHER' AND
				E_SHIFT = 'MORNING' AND
				SHIFT_DATE = NEW.SHIFT_DATE)
		THEN SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'There is already a Dishwasher scheduled for this time';
		END IF;
	END $$

DELIMITER ; 

-- Sample code of trigger stopping insert
INSERT IGNORE INTO EMPLOYEES(E_FIRSTNAME, E_LASTNAME, E_DOB, E_ADDRESS, E_PHONE, E_TITLE)
VALUES
    ('TEST','TESTER','01/06/1959','1 Linguini Court Paris, France','818-325-5563','Dishwasher');
UPDATE EMPLOYEES 
SET E_SHIFT = 
    (CASE
    WHEN E_FIRSTNAME = 'TEST' THEN 'MORNING'
    ELSE E_SHIFT
    END);
INSERT INTO SHIFTS(EMPLOYEEID, SHIFT_DATE)
VALUES
    (17,'2016-10-31');

-- Sample code of trigger allowing correct insert
INSERT IGNORE INTO EMPLOYEES(E_FIRSTNAME, E_LASTNAME, E_DOB, E_ADDRESS, E_PHONE, E_TITLE)
VALUES
    ('TEST','TESTER','01/06/1959','1 Linguini Court Paris, France','818-325-5563','Dishwasher');
UPDATE EMPLOYEES 
SET E_SHIFT = 
    (CASE
    WHEN E_FIRSTNAME = 'TEST' THEN 'MORNING'
    ELSE E_SHIFT
    END);
INSERT INTO SHIFTS(EMPLOYEEID, SHIFT_DATE)
VALUES
    (17,'2016-10-30');
SELECT * FROM SHIFTS WHERE EMPLOYEEID = 17;




