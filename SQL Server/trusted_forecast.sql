truncate table refine.trusted_demand_forecast
insert into refine.trusted_demand_forecast
select 
	[date]
	,plant_id
	,fini_product_id
	,customer_id
	,country_id
	,round(sum(forecast_kg),2) as forecast_kg
from refine.auxiliary_demand_kg
where [date] >= 202205
group by [date]
	,plant_id
	,fini_product_id
	,customer_id
	,country_id