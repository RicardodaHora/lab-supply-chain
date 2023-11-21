insert into refine.trusted_country
select country,region_id from
(select distinct
Customer_Country as country
,case when  Customer_Region is null and Customer_Subregion = 'US & CANADA' then 'NAM' 
	  when Customer_Country = 'BQ' then 'LAM'
	  when Customer_Country = 'CG' then 'EMEA'
else Customer_Region end as region
from raw.RAW_AZGNCU_A012
where Customer_Country is not null) country
inner join refine.trusted_region  on country.region = trusted_region.region 


ALTER TABLE [refine].[trusted_country] ADD FOREIGN KEY ([region_id]) REFERENCES [refine].[trusted_region] ([region_id])



