insert into refine.trusted_bu
select distinct Document_Business_Unit as bu from raw.RAW_AILMAT_A012
order by 1

insert into refine.trusted_strategic_bu
select distinct Document_Strategic_Business_Unit as strategic_bu from raw.RAW_AILMAT_A012
order by 1

insert into refine.trusted_division
select distinct Document_Divisions as [division] from raw.RAW_AILMAT_A012
order by 1

insert into refine.[trusted_product_group]
select distinct Document_Product_Group as [product_group] from raw.RAW_AILMAT_A012
order by 1

insert into refine.[trusted_semi_product]
select distinct Semi_Product_Code as [semi_product] from raw.RAW_AILMAT_A012
order by 1

insert into refine.[trusted_fini_product_code]
select distinct Finish_Product_Code as [fini_product] from raw.RAW_AILMAT_A012
order by 1

insert into [refine].[trusted_finish_products]
select 
  [fini_product_id],
  [semi_product_id],
  [strategic_bu_id],
  [division_id],
  [bu_id],
  [product_group_id]
from raw.RAW_AILMAT_A012 a
inner join refine.trusted_fini_product_code b on a.Finish_Product_Code = b.[fini_product]
inner join refine.[trusted_semi_product] c on a.Semi_Product_Code = c.[semi_product]
inner join refine.[trusted_product_group] d on a.Document_Product_Group = d.[product_group]
inner join refine.trusted_bu e on a.Document_Business_Unit = e.bu
inner join refine.trusted_division f on a.Document_Divisions = f.[division]
inner join refine.trusted_strategic_bu g on a.Document_Strategic_Business_Unit = g.strategic_bu
order by 1,2,3,4,5,6


ALTER TABLE [refine].[trusted_finish_products] ADD FOREIGN KEY ([fini_product_id]) REFERENCES [refine].[trusted_fini_product_code] ([fini_product_id])
GO
ALTER TABLE [refine].[trusted_finish_products] ADD FOREIGN KEY ([product_group_id]) REFERENCES [refine].[trusted_product_group] ([product_group_id])
GO
ALTER TABLE [refine].[trusted_finish_products] ADD FOREIGN KEY ([semi_product_id]) REFERENCES [refine].[trusted_semi_product] ([semi_product_id])
GO
ALTER TABLE [refine].[trusted_finish_products] ADD FOREIGN KEY ([strategic_bu_id]) REFERENCES [refine].[trusted_strategic_bu] ([strategic_bu_id])
GO
ALTER TABLE [refine].[trusted_finish_products] ADD FOREIGN KEY ([bu_id]) REFERENCES [refine].[trusted_bu] ([bu_id])
GO
ALTER TABLE [refine].[trusted_finish_products] ADD FOREIGN KEY ([division_id]) REFERENCES [refine].[trusted_division] ([division_id])
GO