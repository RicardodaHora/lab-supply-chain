---202205 + forecast / 202205 - historical

drop table if exists #x
select 
Calendar_Year_Month as [date]
,f.plant_id
,b.fini_product_id
,e.customer_id
,d.country_id
,a.Consensus_Forecast_KG as forecast_kg
into #x
from [raw].[RAW_AAPO_A012] a
inner join refine.trusted_fini_product_code b on a.Finish_Product_Code = b.fini_product
inner join refine.trusted_customer_id c on a.Native_Shipto_Customer = c.customer
inner join refine.trusted_customer d on d.customer_id = c.customer_id
inner join refine.trusted_customer_id e on a.Native_Soldto_Customer = e.customer
inner join refine.trusted_plant_id f on a.plant = f.plant

select * from #x

drop table if exists #xx
select 
	[date]
	,plant_id
	,fini_product_id
	,customer_id
	,country_id
	,case when forecast_kg like '%.%.%' then STUFF(forecast_kg, charindex('.',forecast_kg,CHARINDEX('.', forecast_kg,1)+1), 1, '') else forecast_kg end as forecast_kg
into #xx
from #x

drop table if exists #xxx
select 
cast(date as int) as [date]
,plant_id
,fini_product_id
,customer_id
,country_id
,case when forecast_kg like '%.%.%' then STUFF(forecast_kg, charindex('.',forecast_kg,CHARINDEX('.', forecast_kg,1)+1), 1, '') else forecast_kg end as forecast_kg
into #xxx
from #xx

drop table if exists #x2
select
	[date]
	,plant_id
	,fini_product_id
	,customer_id
	,country_id
	,case when CHARINDEX('-',forecast_kg) >0 then CONCAT('-',replace(forecast_kg,'-','')) else forecast_kg end as forecast_kg
	into #x2
from #xxx

truncate table refine.auxiliary_demand_kg
insert into refine.auxiliary_demand_kg
select 
	[date]
	,plant_id
	,fini_product_id
	,customer_id
	,country_id
	,100 * cast(forecast_kg as float) as forecast_kg
from #x2
