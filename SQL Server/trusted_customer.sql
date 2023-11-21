insert into [refine].[trusted_customer_id]
select distinct
	REPLACE(LTRIM(REPLACE(Native_Customer, '0', ' ')), ' ', '0')as  customer
from raw.RAW_AZGNCU_A012
order by 1

select 
	REPLACE(LTRIM(REPLACE(Native_Customer, '0', ' ')), ' ', '0')as  customer
	,Customer_Country
	,Customer_Subregion
	,case when Customer_Region is null and Customer_Subregion = 'US & CANADA' then 'NAM' 
		  when Customer_Region = 'ASIA' then 'ASPAC'
		  when Customer_Region = 'LA' then 'LAM'
		  else Customer_Region
	end as region
	into #x
from raw.RAW_AZGNCU_A012
order by 1

insert into [refine].[trusted_subregion_id]
select distinct Customer_Subregion as subregion from #x
order by 1

insert into [refine].[trusted_customer]
select distinct
	customer_id
	,country_id
	,region_id
	,subregion_id
from #x a
inner join [refine].[trusted_customer_id] b on a.customer = b.customer
inner join refine.trusted_country c on a.Customer_Country = c.country
inner join refine.[trusted_subregion_id] d on a.Customer_Subregion = d.subregion


ALTER TABLE [refine].[trusted_customer] ADD FOREIGN KEY ([subregion_id]) REFERENCES [refine].[trusted_subregion_id] ([subregion_id])

ALTER TABLE [refine].[trusted_customer] ADD FOREIGN KEY ([customer_id]) REFERENCES [refine].[trusted_customer_id] ([customer_id])
GO

ALTER TABLE [refine].[trusted_customer] ADD FOREIGN KEY ([country_id]) REFERENCES [refine].[trusted_country] ([country_id])
GO

ALTER TABLE [refine].[trusted_customer] ADD FOREIGN KEY ([region_id]) REFERENCES [refine].[trusted_region] ([region_id])