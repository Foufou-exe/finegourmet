CREATE TABLE
  `Dim_Client` (
    `Client_ID` INT PRIMARY KEY,
    `Email` VARCHAR(100),
    `Last_Name` VARCHAR(100),
    `First_Name` VARCHAR(100),
    `Phone` VARCHAR(50),
    `Address` VARCHAR(255)
  );

CREATE TABLE
  `Dim_Product` (
    `Product_ID` VARCHAR(50) PRIMARY KEY,
    `Name` VARCHAR(255) NOT NULL,
    `Category` VARCHAR(255),
    `Price` DECIMAL(10, 2) NOT NULL
  );

CREATE TABLE
  `Dim_Store` (
    `Store_ID` VARCHAR(50) PRIMARY KEY,
    `Name` VARCHAR(255) NOT NULL,
    `Address` VARCHAR(255) NOT NULL
  );

CREATE TABLE
  `Fact_Sales` (
    `Sale_ID` VARCHAR(50) PRIMARY KEY,
    `Quantity` INT NOT NULL,
    `Price` DECIMAL(10, 2) NOT NULL,
    `Type` VARCHAR(50) NOT NULL,
    `Date` DATE NOT NULL,
    `FK_Client_ID` INT,
    `FK_Product_ID` VARCHAR(50) NOT NULL,
    `FK_Store_ID` VARCHAR(50)
  );

ALTER TABLE `Fact_Sales` ADD FOREIGN KEY (`FK_Client_ID`) REFERENCES `Dim_Client` (`Client_ID`);

ALTER TABLE `Fact_Sales` ADD FOREIGN KEY (`FK_Product_ID`) REFERENCES `Dim_Product` (`Product_ID`);

ALTER TABLE `Fact_Sales` ADD FOREIGN KEY (`FK_Store_ID`) REFERENCES `Dim_Store` (`Store_ID`);