BEGIN TRAN

CREATE TABLE refine.trusted_inventory (date int, plant_id int, fini_product_id int, region_id int, qtd_kg float, safety_stock float)

SELECT A.[Calendar_Year_Month]
	   ,PID.[plant_id]
       ,PC.[fini_product_id]
	   ,C.[region_id]
	   ,REPLACE(A.Stock_on_Hand_Unrestricted, ',', '.') AS Stock_on_Hand_Unrestricted_F
	   ,A.[Stock_on_Hand_Restricted]
	   ,A.[Additional_Demand]
	   ,REPLACE(A.[In_Transit], ',', '.') AS In_Transit_F
	   ,REPLACE(A.[Current_Safety_Stock], ',', '.') AS [Current_Safety_Stock_F]
	   ,A.[Current_Reorder_Point]

		--Regra de normalizacao
		INTO #temp

  FROM [raw].[RAW_AAPO_A252] A
  INNER JOIN [refine].[trusted_plant_id] PID ON A.[Plant] = PID.[Plant]
  INNER JOIN [refine].[trusted_fini_product_code] PC ON A.[Finish_Product_Code] = PC.[fini_product]
  INNER JOIN [refine].[trusted_plant] P  ON PID.[plant_id] = P.[plant_id]
  INNER JOIN [refine].[trusted_country] C ON P.[country_id] = C.[country_id]
WHERE [Calendar_Year_Month] >= '202205' AND [Stock_on_Hand_Unrestricted] NOT LIKE '%e%' ORDER BY 1


--Soma das colunas
INSERT INTO refine.trusted_inventory (date, plant_id, fini_product_id, region_id, qtd_kg, safety_stock)
SELECT  [Calendar_Year_Month]
       ,[plant_id]
       ,[fini_product_id]
	   ,[region_id]
	   ,SUM(CAST(Stock_on_Hand_Unrestricted_F AS FLOAT) + CAST(In_Transit_F AS FLOAT)) AS qtd_kg
	   ,MAX(CAST([Current_Safety_Stock_F] AS FLOAT)) AS safety_stock
FROM #temp
GROUP BY  [Calendar_Year_Month]
         ,[plant_id]
         ,[fini_product_id]
	     ,[region_id];


SELECT *
FROM refine.trusted_inventory ORDER BY 1

ROLLBACK

