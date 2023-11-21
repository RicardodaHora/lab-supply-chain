drop table if exists #x
select 
  a.Finish_Product_Code
 ,REPLACE(LTRIM(REPLACE(a.Native_Shipto_Customer, '0', ' ')), ' ', '0') as  shipto_customer
 ,REPLACE(LTRIM(REPLACE(a.Native_Soldto_Customer, '0', ' ')), ' ', '0') as  soldto_customer
 ,cast(a.Calendar_Year_Month as int) as 'date'
 ,a.Plant
 ,a.Posting_type
 ,a.[Sales_Qty_incl Captive]
 ,a.[Sales]
 ,a.[Std_Cost]
 ,a.[Gross_Profit_at_Std]
 into #x
from [raw].[RAW_APGP3_A012] a

drop table if exists #x2
select 
  [date]
  ,e.plant_id
  ,b.fini_product_id
  ,d.customer_id as soldto_customer
  ,c.customer_id as shipto_customer
  ,a.Posting_type
  ,case 
		when a.[Sales_Qty_incl Captive] like '%-' 
		then cast(concat('-',replace(a.[Sales_Qty_incl Captive], '-', '')) as float)
		else cast(a.[Sales_Qty_incl Captive] as float) 
   end as [Sales_Qty_incl Captive]
   ,case 
		when a.[Sales] like '%-' 
		then cast(concat('-',replace(a.[Sales], '-', '')) as float)
		else cast(a.[Sales] as float) 
   end as [Sales]
   ,case 
		when a.[Std_Cost] like '%-' 
		then cast(concat('-',replace(a.[Std_Cost], '-', '')) as float)
		else cast(a.[Std_Cost] as float) 
   end as [Std_Cost]
   ,case 
		when a.[Gross_Profit_at_Std] like '%-' 
		then cast(concat('-',replace(a.[Gross_Profit_at_Std], '-', '')) as float)
		else cast(a.[Gross_Profit_at_Std] as float) 
   end as [Gross_Profit_at_Std]
into #x2
from #x a
inner join [refine].[trusted_fini_product_code] b on a.[Finish_Product_Code] = b.[fini_product]
inner join [refine].[trusted_customer_id] c on a.shipto_customer = c.[customer]
inner join [refine].[trusted_customer_id] d on a.soldto_customer = d.[customer]
inner join [refine].[trusted_plant_id] e on cast(a.Plant as varchar) = e.[plant]

drop table if exists #base1
select 
  [date]
  ,[fini_product_id]
  ,plant_id
  ,soldto_customer as customer_id
  ,country_id
  ,sum(a.[Sales_Qty_incl Captive]) as sales_qtd_kg
  ,sum(a.[Sales]) as sales_reve_usd
  ,sum(a.[Std_Cost]) as sales_cost_usd
into #base1
from #x2 a
inner join [refine].[trusted_customer] b on a.shipto_customer = b.customer_id
group by
 [date]
,[fini_product_id]
,soldto_customer
,country_id
,plant_id

drop table if exists #price_corr
select 
	fini_product_id
	,customer_id
	,country_id
	,avg(sales_reve_usd/sales_qtd_kg) as price_kg
	,avg(sales_cost_usd/sales_qtd_kg) as cost_kg
into #price_corr
from #base1
where sales_qtd_kg <> 0 and sales_reve_usd <> 0 and sales_cost_usd <> 0 
group by
	 fini_product_id
	,customer_id
	,country_id
	
drop table if exists #historical_aux
select 
	a.[date]
	,a.plant_id
	,a.fini_product_id
	,a.customer_id
	,a.country_id
	,forecast_kg as sales_qtd_kg
	,forecast_kg * price_kg as sales_reve_usd
	,forecast_kg * cost_kg as sales_cost_usd
	into #historical_aux
from [refine].[auxiliary_demand_kg] a
inner join #price_corr b on a.fini_product_id = b.fini_product_id and a.customer_id = b.customer_id and a.country_id = b.country_id
where b.fini_product_id is not null and [date] < 202206

drop table if exists #base2
select 
	*  
into #base2 
from #base1
	union
select 
	*
from #historical_aux
order by 1

create table refine.trusted_historical_sales 
([date] int, plant_id int,
fini_product_id int, customer_id int,
country_id int, sales_qtd_kg float,
sales_reve_usd float, sales_cost_usd float)

insert into refine.trusted_historical_sales
select 
	[date]
	,plant_id
	,fini_product_id
	,customer_id
	,country_id
	,sales_qtd_kg
	,sales_reve_usd
	,sales_cost_usd
from #base2
