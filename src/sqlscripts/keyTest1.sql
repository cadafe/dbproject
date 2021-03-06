CREATE TABLE IF NOT EXISTS TableA (
	a1 INT PRIMARY KEY,
    a2 INT,
    a3 FLOAT
);
CREATE TABLE IF NOT EXISTS TableX (
	x1 INT,
    x2 INT PRIMARY KEY,
    x3 FLOAT
);
CREATE TABLE IF NOT EXISTS TableB (
	b1 INT PRIMARY KEY,
    b2 VARCHAR(30), 
    b3 INT,
    b4 INT,
    CONSTRAINT fk_TableA FOREIGN KEY (b4)
    REFERENCES TableA(a1)
    ON DELETE CASCADE,
    CONSTRAINT fk_TableX FOREIGN KEY (b3)
    REFERENCES TableX(x2)
);