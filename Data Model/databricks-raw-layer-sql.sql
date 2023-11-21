CREATE SCHEMA [raw]
GO

CREATE TABLE [raw].[raw_customers_data] (
  [Native_Customer] string,
  [Customer_Country] string,
  [Customer_Subregion] string,
  [Customer_Region] string,
  [Customer_Currency_key] string
)
GO

CREATE TABLE [raw].[raw_ferm_transportation] (
  [plant_id] int,
  [g_L_ferm_density] double,
  [Ferm_Code] string
)
GO

CREATE TABLE [raw].[raw_finish_products] (
  [Finish_Product_Code] string,
  [Document_Strategic_Business_Unit] string,
  [Document_Divisions] string,
  [Document_Business_Unit] string,
  [Document_Product_Group] string,
  [Semi_Product_Code] string
)
GO

CREATE TABLE [raw].[raw_forecast_demand_data] (
  [Calendar_Year_Month] string,
  [Plant] string,
  [Finish_Product_Code] string,
  [Native_Soldto_Customer] string,
  [Native_Shipto_Customer] string,
  [Baseline_Forecast_KG] string,
  [Consensus_Forecast_KG] string
)
GO

CREATE TABLE [raw].[raw_freight] (
  [Mode_of_trans] string,
  [Ship_from_Region] string,
  [Shipto_Region] string,
  [Sum_of_No_of_Shipments] string,
  [Sum_of_Net_weight] string,
  [Sum_of_Total_Freight_Cost_USD] string,
  [Sum_of_usd_Kg] string
)
GO

CREATE TABLE [raw].[raw_historical_data] (
  [Finish_Product_Code] string,
  [Native_Shipto_Customer] string,
  [Native_Soldto_Customer] string,
  [Calendar_Year_Month] int,
  [Plant] double,
  [Posting_type] string,
  [Calendar_Year] int,
  [Sales_Qty_incl_Captive] string,
  [Sales] string,
  [Std_Cost] string,
  [Gross_Profit_at_Std] string
)
GO

CREATE TABLE [raw].[raw_inventory_data] (
  [Calendar_Year_Month] string,
  [Finish_Product_Code] string,
  [Plant] string,
  [Stock_on_Hand_Unrestricted] string,
  [Stock_on_Hand_Restricted] string,
  [Additional_Demand] string,
  [In_Transit] string,
  [Current_Safety_Stock] string,
  [Current_Reorder_Point] string
)
GO

CREATE TABLE [raw].[raw_make_site] (
  [Semi_Product_Code] string,
  [Ferm_Code] string,
  [Producing_Plant] int,
  [Finish_Plant] int,
  [gL_Factor] double,
  [region] string
)
GO

CREATE TABLE [raw].[raw_plant_capacity] (
  [plant] int,
  [max_volume_coapacity] double
)
GO

CREATE TABLE [raw].[raw_plants_data] (
  [Plant] string,
  [Country] string
)
GO

CREATE TABLE [raw].[raw_products_components] (
  [Semi_Product_Code] string,
  [no_of_Components] double,
  [Comp1] string,
  [Comp2] string,
  [Comp3] string,
  [Comp4] string,
  [Comp5] string,
  [Comp6] string,
  [Region] string
)
GO

CREATE TABLE [raw].[raw_recovery_data] (
  [id] int,
  [plant] int,
  [ferm_code] string,
  [run_time] double,
  [machine_volume_L] double,
  [bactch_output_g_l] double
)
GO

CREATE TABLE [raw].[raw_semi_finish_data] (
  [id] int,
  [plant] int,
  [ferm_code] string,
  [Run_Time] double,
  [Machine_Volume_L] double,
  [Successful_Batch_Output_g_L] double
)
GO
