# Full Data Ingestion into Fabric

Here we will go through end to end of moving a full set of data into Fabric. Moving through the traditional data lakehouse zones. And finally transforming our data into a basic star schema. 

### Step 1: Load Raw Data with a Pipeline
Initial data ingestion is made simple in Fabric via the *Copy data assistant*. This allows us to quickly move data from a supported source directly into Fabric via a wizard based process.

#### Step 1.1
Create a new pipeline in workspace **(1) Raw**. Please name it `Full Raw Data Ingestion`. 

#### Step 1.2
Once your new pipeline is created Click the *Copy data assistant* tile. 

With the Wizard open, select a data source of *Azure SQL Database*.

Select Create new connection and put in the details about the database provided in the video.

Once connected you will see a list of tables, please check/select:

| Tables                         |
|--------------------------------|
| dbo.movies                     |
| dbo.directors                  |
| dbo.actors                     |
| dbo.genres                     |
| dbo.criticreviews              |
| dbo.directorstomoviesjoin      |
| dbo.actorstomoviesjoin         |
| app.Rentals                    |
| app.Kiosk                      |
| app.Users                      |
| app.UserAddresses              |
| app.UserCreditCards            |
| app.Inventory                  |
| app.Purchases                  |
| app.UserReviews                |
| app.PurchaseLineItems          |
| app.PurchaseUser               |
| app.PurchaseUserCreditCard     |
| app.UserSubscriptionStatus     |
| app.Returns                    |

Next, select the Data Destination of Lakehouse and then select the BronzeLakehouse in the dropdown.

In the connecting to the data destination section, select the Files radio button, and in the folder path type `InitialLoad/`. In the File name suffix section type `.parquet`. Keep the defaults on the next screen. 

Finally on Review + Save, make sure "Start data transfer immediately" is selected, and then click *Save + Run*.

Once the pipeline has finished running you now have ingested your Raw Data!

### Step 2: Conforming and Curating your Data

Now using Shortcuts and Spark we will be able to quickly conform our data into standardized formats and then Curate our data into a dimensionalized model.

#### Step 2.1

First to access our data we have to create a Shortcut from our SilverLakehouse to our BrozeLakehouse. 

Go to the **(2) Conformed** workspace and click into the SilverLakehouse. Click the three dots by the *Files* folder and click New shortcut.

    Hint: If you click by the *Tables* folder, the shortcut will not work.

Select the Microsoft OneLake tile on the New shortcut wizard. Then select BronzeLakehouse in the list provided. Expand the Files node and click the checkbox next to the InitialLoad folder. Finally, click Create. 

You will see the InitialLoad folder in the Files section of the SilverLakehouse now.

#### Step 2.2

Now let's import the Spark Notebooks that will let us conform our parquet files into delta tables. 

Right click on each link below and right click and save the notebooks to your local computer. 

- [(Full Load) Conform Parquet to Delta](Notebooks/(Full%20Load)%20Conform%20Parquet%20To%20Delta.ipynb?raw=true)
- [FullSingleTable_ForParallel](Notebooks/FullSingleTable_ForParallel.ipynb?raw=true)
- [(Full Load) Curate Data](Notebooks/(Full%20Load)%20Curate%20Data.ipynb?raw=true)

Next go into your **(2) Conformed** workspace, click the *+ New* button and select *Import notebook*. Click the upload button to select all three notebooks for uploading.

Once they have Uploaded, open the three notebooks and change the default lakehouse to be your SilverLakehouse.

#### Step 2.3

Now let's run the initial load, open the Notebook named `(Full Load) Conform Parquet To Delta`.

Assuming you have properly added the Default Lakehouse to your notebooks, push the Run all button and observe the loading process, this should take a minute or two.

Once complete, all progress bars should be green.

#### Step 2.4

Next we will Curate our data into a star schema.

First let's add a shortcut to our newly created tables in our SilverLakehouse to our CuratedLakehouse. 

Open the CuratedLakehouse and click the three dots by the Tables folder. Click New shortcut. 

Click the OneLake tile and select the SilverLakehouse.

Expand the Tables node and select all the tables, click Next and then Create.

Once your shortcut has been added, go back to the **(2) Conformed** workspace. 

Open the `(Full Load) Curate Data` notebook you imported in Step 2.2 and add the CuratedLakehouse as the default lakehouse. 

Click Run all to curate your data. If you would like you can read the Markdown comments to learn more about what each section of the notebook is doing.

This run will take approximately 5 minutes and once finished you will have 6 new tables with your now curated data.

### Step 3: Moving your data to the production warehouse

As our final step we will move our data from our curated lakehouse into our production data warehouse.

#### Step 3.1

First let's navigate to the **(3) Production** workspace. 

Let's then add a shortcut from our Curated tables in our **(2) Curated** workspace to our **(3) Production** workspace. Open the CuratedLakehouse in the production workspace and click the three dots after the Tables folder. Click New shortcut.

Click the OneLake tile, then select the CuratedLakehouse in the **(2) Conformed** workspace. Expand the tables node and click the checkboxes to select the tables listed below.

| Tables                         |
|--------------------------------|
| dimdate                        |
| dimkiosk                       |
| dimmovies                      |
| dimuser                        |
| factpurchases                  |
| factrentals                    |

Once you have selected the 6 tables click Next and then Create.

#### Step 3.2

Now that you have the tables shortcutted to your production lakehouse, let's load the data into your warehouse.

Open the ProductionWarehouse and click New SQL query.

Copy the queries from the file below and click run. 

- [(Full Load) Production Table Creation](Queries/(Full%20Load)%20Production%20Table%20Creation.sql)

You have now loaded your production warehouse with your data.

### Complete!

You have finished doing a full data load end to end in Fabric! Maybe try making a report based on the data and see what you can find out. 

Or move on to the Delta Ingestion process to have always up to date data!