insert into refine.trusted_ferm_id
select distinct ferm_code from raw.RAW_RECOVTA a
inner join (select min(id) as id from raw.RAW_RECOVTA
group by plant,ferm_code) b on a.id = b.id
order by 1

insert into refine.[trusted_ferm]
select
	plant_id
	,ferm_code_id
	,run_time
	,machine_volume_L
	,bactch_output_g_l as batch_output_g_l
from raw.RAW_RECOVTA a
inner join (select min(id) as id from raw.RAW_RECOVTA
group by plant,ferm_code) b on a.id = b.id
inner join refine.trusted_plant_id c on cast(a.plant as varchar) = c.plant
inner join refine.trusted_ferm_id d on a.ferm_code = d.ferm_code
order by 1

insert into [refine].[trusted_recipes]
select distinct b.[semi_product_id]
,no_of_Components as total_components
,region_id
,comp_number
,c.ferm_code_id
,gL_Factor
from (select Semi_Product_Code,no_of_Components,Region,Comp1,Comp2,Comp3,Comp4,Comp5,Comp6
from raw.RAW_BLENDS) t
unpivot (ferm_code for comp_number in (Comp1,Comp2,Comp3,Comp4,Comp5,Comp6) ) as unpvt
inner join [refine].[trusted_semi_product] b on unpvt.Semi_Product_Code = b.[semi_product]
inner join refine.trusted_ferm_id c on unpvt.ferm_code = c.ferm_code
inner join refine.trusted_region d on unpvt.Region = d.region
inner join (select distinct Semi_Product_Code, Ferm_Code,gL_Factor,region from raw.RAW_MAKE_SITES) ms on ms.Ferm_Code = unpvt.ferm_code
and ms.region = unpvt.Region and ms.Semi_Product_Code = unpvt.Semi_Product_Code
order by 1,3,4

insert into [refine].[trusted_ferm_density]
select distinct b.plant_id,c.ferm_code_id,max(a.g_L_ferm_density) as g_L_ferm_density from raw.RAW_FERM_TRANSPORTATION a
inner join refine.trusted_plant_id b on cast(a.plant_id as varchar) = b.plant
inner join refine.trusted_ferm_id c on a.Ferm_Code = c.ferm_code
group by b.plant_id,c.ferm_code_id
order by 1,2

ALTER TABLE [refine].[trusted_ferm_density] ADD FOREIGN KEY ([ferm_code_id]) REFERENCES [refine].[trusted_ferm_id] ([ferm_code_id])
GO

ALTER TABLE [refine].[trusted_ferm] ADD FOREIGN KEY ([ferm_code_id]) REFERENCES [refine].[trusted_ferm_id] ([ferm_code_id])
GO

ALTER TABLE [refine].[trusted_recipes] ADD FOREIGN KEY ([ferm_code_id]) REFERENCES [refine].[trusted_ferm_id] ([ferm_code_id])

ALTER TABLE [refine].[trusted_recipes] ADD FOREIGN KEY ([region_id]) REFERENCES [refine].[trusted_region] ([region_id])
GO

ALTER TABLE [refine].[trusted_recipes] ADD FOREIGN KEY ([semi_product_id]) REFERENCES [refine].[trusted_semi_product] ([semi_product_id])
GO

