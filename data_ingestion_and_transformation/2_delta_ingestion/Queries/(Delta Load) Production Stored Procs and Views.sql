CREATE SCHEMA stg;

CREATE OR ALTER PROCEDURE UpsertDimUsers
AS
BEGIN
    -- Insert all new records
    INSERT INTO dbo.DimUsers 
    SELECT  *
        FROM    CuratedLakehouse.dbo.dimuser l
        WHERE   NOT EXISTS
        (
            SELECT  NULL
            FROM    DimUsers r
            WHERE   r.UserId = l.UserId
        )

    -- Remove our staging table if it exists
    DROP TABLE IF EXISTS stg.UpdatedUsers;

    -- Create a staging table with all the updated records
    CREATE TABLE stg.UpdatedUsers AS 
        SELECT * FROM CuratedLakehouse.dbo.dimuser
    EXCEPT
        SELECT * FROM dbo.DimUsers;

    -- Update all the records that have been updated
    UPDATE u 
        SET u.FirstName = uu.FirstName,
        u.LastName = uu.LastName,
        u.Email = uu.Email,
        u.PhoneNumber = uu.PhoneNumber,
        u.MemberSinceDateId = uu.MemberSinceDateId,
        u.Address = uu.Address,
        u.State = uu.State,
        u.ZipCode = uu.ZipCode,
        u.RenewalDay = uu.RenewalDay,
        u.IsSubscriptionActive = uu.IsSubscriptionActive
    FROM dbo.DimUsers u
        INNER JOIN stg.UpdatedUsers uu ON u.UserId = uu.UserId
END


CREATE OR ALTER PROCEDURE RecreateDimKiosks
AS
BEGIN
    DROP TABLE IF EXISTS dbo.DimKiosks;
    CREATE TABLE dbo.DimKiosks AS SELECT * FROM CuratedLakehouse.dbo.dimkiosk;
END



CREATE OR ALTER PROCEDURE UpsertDimMovies
AS
BEGIN
    -- Insert all new records into the DimMovies table
    INSERT INTO dbo.DimMovies
    SELECT  *
        FROM    CuratedLakehouse.dbo.dimmovies l
    WHERE   NOT EXISTS
    (
        SELECT  NULL
        FROM    DimMovies r
        WHERE   r.MovieId = l.MovieId
    )
END



CREATE OR ALTER PROCEDURE UpsertFactPurchases
AS
BEGIN
    INSERT INTO dbo.FactPurchases
    SELECT  *
        FROM    CuratedLakehouse.dbo.factpurchases l
    WHERE   NOT EXISTS
    (
        SELECT  NULL
        FROM    dbo.FactPurchases r
        WHERE   r.PurchaseLineItemId = l.PurchaseLineItemId
    )
END

-- Some extra commentary on how we are creating the views and stored procedures for the factrentals table
-- We are creating a view that will give us the latest rental facts for each rental id
-- We are then creating a stored procedure that will upsert the factrentals table
-- This is very different than what you would do in a traditional data warehouse as you would be storing
-- the full history in the fact table, but we are essentially doing a type 1 slowly changing dimension
-- in the fact table. This is because we are using a lakehouse and we can easily access the full history
-- from the curated lakehouse if we need to. And we are only interested in the latest facts in the fact table,
-- which allows us to more efficiently query the fact table.
CREATE OR ALTER VIEW dbo.LatestRentalFacts
AS
    with cte as (
        select row_number() over (partition by RentalId
        order by DateModified desc) as rn, * 
        from CuratedLakehouse.dbo.factrentals ) 
    select RentalId, MovieId, UserId, RentalLocationId, RentalDateId, ExpectedReturnDateId, ReturnDateId, ReturnLocationId, LateDays, RentalPrice, LateFee, TotalPrice
    from cte
    where rn=1;


CREATE OR ALTER PROCEDURE UpsertFactRentals
AS
BEGIN
    INSERT INTO dbo.FactRentals
    SELECT  *
        FROM    dbo.LatestRentalFacts l
    WHERE   NOT EXISTS
    (
        SELECT  NULL
        FROM    dbo.FactRentals r
        WHERE   r.RentalId = l.RentalId
    )

    DROP TABLE IF EXISTS stg.UpdatedRentals;

    CREATE TABLE stg.UpdatedRentals AS 
        SELECT * FROM dbo.LatestRentalFacts
    EXCEPT
        SELECT * FROM dbo.FactRentals;

    UPDATE r
    SET
        r.ReturnDateId = ur.ReturnDateId,
        r.ReturnLocationId = ur.ReturnLocationId,
        r.LateDays = ur.LateDays,
        r.RentalPrice = ur.RentalPrice,
        r.LateFee = ur.LateFee,
        r.TotalPrice = ur.TotalPrice
    FROM dbo.FactRentals r
        INNER JOIN stg.UpdatedRentals ur ON r.RentalId = ur.RentalId
END
