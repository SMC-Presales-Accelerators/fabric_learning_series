# End to End Data Ingestion and Transformation

In this module we will walk through moving data from a SQL Azure Database to Microsoft Fabric. We will then walk through transforming the data from raw layer. 

## Prequisites

Database Access - You can request database credentials @ [SMC Presales Accelerator Resources](https://smcacceleratorresources.azurewebsites.net/), you will just need a Github login to start.

Three (3) Workspaces in Fabric backed by Trial or Provisioned capacity. The numbers are added at the beginning of the workspace names to keep them at the top of the UI.

Workspace Names:
- (1) Raw
- (2) Conformed
- (3) Production

Inside the **(1) Raw** workspace create two items:
- A Lakehouse named BronzeLakehouse
- A Warehouse named VersionTracking

Inside the **(2) Conformed** workspace create two items:
- A Lakehouse named SilverLakehouse
- A Lakehouse named CuratedLakehouse

Inside the **(3) Production** workspace create two items:
- A Lakehouse named CuratedLakehouse
- A Warehouse named ProductionWarehouse

You now have everything you need to get started! Let's move on to the first steps of ingesting our full dataset.

- [Full Data Ingestion](1_full_ingestion/README.md)
- [Delta Data Ingestion](2_delta_ingestion/README.md)