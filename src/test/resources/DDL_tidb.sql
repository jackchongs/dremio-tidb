CREATE  TABLE all_types (
A BIT(5),
B BIGINT,
C BIGINT UNSIGNED,
D BINARY(4),
E BLOB,
F CHAR(4),
G DATE,
H DATETIME,
I DECIMAL UNSIGNED,
J DOUBLE,
K DOUBLE PRECISION,
L ENUM('RED','GREEN','BLUE'),
M FLOAT,
N INT,
O INT UNSIGNED,
P INTEGER,
Q INTEGER UNSIGNED,
R LONG VARBINARY,
S LONG VARCHAR,
T LONGBLOB,
U LONGTEXT,
V MEDIUMBLOB,
W MEDIUMINT,
X MEDIUMINT UNSIGNED,
Y MEDIUMTEXT,
Z NUMERIC,
AA REAL,
AB SET('a','b'),
AC SMALLINT,
AD SMALLINT UNSIGNED,
AE TEXT,
AF TIME,
AG TIMESTAMP,
AH TINYBLOB,
AI TINYINT,
AJ TINYINT UNSIGNED,
AK TINYTEXT,
AL VARBINARY(8),
AM VARCHAR(8),
AN YEAR
);

INSERT INTO all_types VALUES (
b'1',
1,
1,
'1',
1,
'c',
'2018-01-01',
'2018-02-20 00:00:00',
1,
1.11,
1.11,
'RED',
1.11,
1,
1,
1,
1,
'1',
'1',
'1',
'1',
'1',
1,
1,
'1',
1.11,
1.11,
'a',
1,
1,
'dremio',
'00:00:00',
'2018-02-20 00:00:00',
1,
1,
'0',
'dremio',
'1',
'dremio',
2019);