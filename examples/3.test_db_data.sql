

--########################################################
--client data

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (1, 'Ivan', 'Vasilevich', '01.01.2001', 'vasya@email.ru', '+71234567890');

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (2, 'Eelaun', 'Masyanin', '02.02.2002', 'enel@email.ru', '+72345678901');

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (3, 'Spider', 'Moon', '03.03.2003', 'spm@imail.com', '+11234567890');

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (4, 'Roofy', 'Carlsson', '04.04.2004', 'carlssonontheroof@bmail.com', '+12345678901');

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (5, 'Mister', 'Andersson', '05.05.1998', 'whiterabbit@qmail.com', '+13456789012');

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (6, 'Elza', 'Quinn', '07.03.1993', 'elyzee@gmail.com', '+94567890123');

INSERT INTO cln.client (client_id, client_firstname, client_lastname, client_birthdate, client_email, client_phone)
VALUES (7, 'Maugli', 'Wolfenson', '07.03.1955', 'balu@gmail.com', '+95678901234');

--########################################################
--staff data

INSERT INTO stf.staff (staff_id, staff_firstname, staff_lastname, staff_birthdate, staff_email, staff_phone)
VALUES (1, 'Toser', 'Venprotect', '06.03.1974', 'police@lapd.com', '+97455363563');

INSERT INTO stf.staff (staff_id, staff_firstname, staff_lastname, staff_birthdate, staff_email, staff_phone)
VALUES (2, 'Miner', 'Crafter', '12.04.1990', 'needmoregold@hmail.com', '+17455363563');

INSERT INTO stf.staff (staff_id, staff_firstname, staff_lastname, staff_birthdate, staff_email, staff_phone)
VALUES (3, 'Alice', 'Wonderlandy', '29.07.1984', 'alicew@qmail.com', '+67455841563');

--########################################################
--branch data

INSERT INTO org.branch(branch_id, branch_code, branch_address, branch_description)
	VALUES (1, 'MSC', 'Russia, Moscow, Tverskaya st., 1', null);

INSERT INTO org.branch(branch_id, branch_code, branch_address, branch_description)
	VALUES (2, 'GRN', 'Greenland, Atammik, Central st., 2', null);

INSERT INTO org.branch(branch_id, branch_code, branch_address, branch_description)
	VALUES (3, 'NYC', 'USA, New York, 5 avenue, 7', $$Top manager's e-mail: super@man.org$$);

--########################################################
--prod data

INSERT INTO org.prod(prod_id, prod_code, prod_name, prod_cost)
    VALUES (1, 'GGG', 'Goge-gola', 10);

INSERT INTO org.prod(prod_id, prod_code, prod_name, prod_cost)
    VALUES (2, 'TSL', 'Tresla', 20);

INSERT INTO org.prod(prod_id, prod_code, prod_name, prod_cost)
    VALUES (3, 'WPH', 'Wi-phone', 30);

--########################################################
--branch data
INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity)
VALUES(1, 1, 1, 10);

INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity)
VALUES(2, 1, 2, 20);

INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity)
VALUES(3, 1, 3, 30);


INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity)
VALUES(4, 2, 1, 40);


INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity, description)
VALUES(5, 3, 1, 50, 'Министерство правды');

INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity, description)
VALUES(6, 3, 2, 60, 'Средний - четкий был детина');

INSERT INTO cln.client_order(client_id, branch_id, prod_id, prod_quantity, description)
VALUES(7, 3, 3, 70, 'Младший вовсе был чувак, а номер карты у него 5258537808753590');



create or replace view cln.v_client_order as
select c.client_firstname
	 , c.client_lastname
	 , c.client_birthdate
	 , c.client_email
	 , c.client_phone
	 , p.prod_code
	 , p.prod_name
	 , p.prod_cost
	 , co.prod_quantity
	 , co.prod_quantity * p.prod_cost as order_cost
	 , b.branch_code
	 , b.branch_address
     , b.branch_description
from cln.client c
join cln.client_order co using(client_id)
join org.prod p using(prod_id)
join org.branch b using(branch_id);

			
			select * from cln.v_client_order