insert into refine.trusted_plant_id
select distinct REPLACE(LTRIM(REPLACE(Plant, '0', ' ')), ' ', '0') as plant from raw.RAW_APLNT_A012
where Country is not null
order by 1

insert into refine.[trusted_plant]
select
	 plant_id
	 ,country_id
	 ,case when Producing_Plant is not null then 1 else 0 end as is_prod
	 ,case when Finish_Plant is not null then 1 else 0 end as is_fini
	 ,case when wh is not null then 1 else 0 end as is_wh
from raw.RAW_APLNT_A012 a
inner join refine.trusted_plant_id b on a.Plant = b.plant
inner join refine.trusted_country c on a.Country = c.country
left join (select distinct cast(Producing_Plant as varchar) as Producing_Plant from raw.RAW_MAKE_SITES) d on a.Plant = d.Producing_Plant
left join (select distinct cast(Finish_Plant as varchar) as Finish_Plant from raw.RAW_MAKE_SITES) e on a.Plant = e.Finish_Plant
left join (select distinct cast(Plant as varchar) as wh from raw.RAW_AAPO_A012 
		   union select distinct cast(Plant as varchar) as wh from raw.RAW_APGP3_A012) f on a.Plant = f.wh
order by 1

ALTER TABLE [refine].[trusted_plant] ADD FOREIGN KEY ([country_id]) REFERENCES [refine].[trusted_country] ([country_id])
GO

ALTER TABLE [refine].[trusted_plant] ADD FOREIGN KEY ([plant_id]) REFERENCES [refine].[trusted_plant_id] ([plant_id])