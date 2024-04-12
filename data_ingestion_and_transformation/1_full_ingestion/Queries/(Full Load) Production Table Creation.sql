-- All these queries are used to create the production tables in the data warehouse. 
-- These tables are created by copying the data from the curated lakehouse using three part naming
-- which lets us directly access delta tables in a lakehouse from a data warehouse.
-- We then use CTAS (Create Table As Select) statement to create the production tables in the data warehouse.

DROP TABLE IF EXISTS DimUsers;
CREATE TABLE dbo.DimUsers AS SELECT * from CuratedLakehouse.dbo.dimuser;

DROP TABLE IF EXISTS dbo.DimDate;
CREATE TABLE dbo.DimDate AS SELECT * FROM CuratedLakehouse.dbo.dimdate;

DROP TABLE IF EXISTS dbo.DimKiosks;
CREATE TABLE dbo.DimKiosks AS SELECT * FROM CuratedLakehouse.dbo.dimkiosk;

DROP TABLE IF EXISTS dbo.DimMovies;
CREATE TABLE dbo.DimMovies AS SELECT * FROM CuratedLakehouse.dbo.dimmovies;

DROP TABLE IF EXISTS dbo.FactPurchases;
CREATE TABLE dbo.FactPurchases AS SELECT * FROM CuratedLakehouse.dbo.factpurchases;

DROP TABLE IF EXISTS dbo.FactRentals;
CREATE TABLE dbo.FactRentals AS SELECT * from CuratedLakehouse.dbo.factrentals;