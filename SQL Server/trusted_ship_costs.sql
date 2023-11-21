BEGIN TRAN

SELECT 
    CASE 
        WHEN Ship_from_region LIKE '%AP%' THEN 'ASPAC'
        WHEN Ship_from_region LIKE '%NA%' THEN 'NAM'
        WHEN Ship_from_region LIKE '%LA%' THEN 'LAM'
        ELSE Ship_from_region
    END AS Ship_from_Region,
    CASE 
        WHEN Shipto_Region LIKE '%AP%' THEN 'ASPAC'
        WHEN Shipto_Region LIKE '%NA%' THEN 'NAM'
        WHEN Shipto_Region LIKE '%LA%' THEN 'LAM'
        ELSE Shipto_Region
    END AS Shipto_Region,
    Mode_of_trans,

CAST(ROUND(CAST(REPLACE(Sum_of_Total_Freight_Cost_USD, ',', '.') AS float) / 
CAST(Sum_of_Net_weight AS float), 2) AS float) AS ship_cost
	

INTO #FREIGHT 
FROM raw.RAW_FREIGHT;


CREATE TABLE refine.trusted_ship_costs (start_region_id int,
end_region_id int, transp_mode_id int, ship_cost float );

INSERT INTO refine.trusted_ship_costs (start_region_id, end_region_id, transp_mode_id, ship_cost)
SELECT 
    TR.region_id AS start_region_id, 
    TR2.region_id AS end_region_id, 
    T.transp_mode_id,
    ship_cost
FROM 
    #FREIGHT F
    INNER JOIN [refine].[trusted_region] TR ON F.Ship_from_Region = TR.region
    INNER JOIN [refine].[trusted_region] TR2 ON F.Shipto_Region = TR2.region
    INNER JOIN [refine].[trusted_transp_mode] T ON F.Mode_of_trans = T.transp_mode;

-- Comando para testar a tabela
SELECT* 
FROM refine.trusted_ship_costs

ROLLBACK


