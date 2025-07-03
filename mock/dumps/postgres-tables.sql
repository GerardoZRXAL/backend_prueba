CREATE TABLE IF NOT EXISTS departments (
department_id VARCHAR NOT NULL,
department_name VARCHAR NOT NULL,
posting_date DATE NOT NULL DEFAULT CURRENT_DATE,
CONSTRAINT departments_pk PRIMARY KEY (department_id)
);

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR NOT NULL,
    user_name VARCHAR NOT NULL,
    posting_date VARCHAR NOT NULL,
    CONSTRAINT users_pk PRIMARY KEY (user_id)
);

INSERT INTO departments (department_id, department_name, posting_date) VALUES 
('01','Papeleria','2021-10-14'),
('02','Computo','2021-10-14'),
('03','Farmacia','2021-10-14');

INSERT INTO users (user_id, user_name, posting_date) VALUES 
('01','Carlos','2021-10-14'),
('02','Perrillo','2021-10-14'),
('03','Cocho','2021-10-14');
