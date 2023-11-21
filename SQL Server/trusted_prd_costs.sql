BEGIN TRAN 

	--Criacao da tabela #x, utilizamos o SUM para retornar dados resumidos, junto do group by.

	DROP TABLE IF EXISTS #x

	SELECT   [date]
			,[plant_id]
			,[fini_product_id]
			,[customer_id]
            ,[country_id]
			,SUM([sales_qtd_kg])'sales_qtd_kg'
			,SUM([sales_cost_usd])'sales_cost_usd'

			INTO #x

		FROM [refine].[trusted_historical_sales]
		GROUP BY
			 [date]
			,[plant_id]
			,[fini_product_id]
			,[customer_id]
            ,[country_id]


	--Criacao da tabela #y, utilizamos o ROW_NUMBER 

	DROP TABLE IF EXISTS #y

	SELECT	 [fini_product_id]
	        ,[plant_id]
			,[sales_qtd_kg]
			,[sales_cost_usd]
			,ROW_NUMBER() OVER(PARTITION BY [fini_product_id],[customer_id],[country_id] ORDER BY [date] DESC) AS Row

			INTO #y

	FROM #x
	WHERE sales_qtd_kg <> 0

	--Criacao da tabela #z, utilizamos esta tabela para fazer a operacao matematica e criacao da nova coluna (sell_price)

	DROP TABLE IF EXISTS #z

	SELECT   [plant_id]
			,[fini_product_id]
			,AVG([sales_cost_usd]/[sales_qtd_kg]) AS prd_cost 
			
			INTO #z
	FROM #y
	WHERE [Row] BETWEEN 1 AND 5
	GROUP BY 
			 [plant_id]
			,[fini_product_id]


	--Criacao da tabela refine.trusted_prd_costs

	CREATE TABLE refine.trusted_prd_costs (plant_id int, fini_product_id int, prd_cost float)


	--Insercao na tabela principal 

	INSERT INTO refine.trusted_prd_costs (plant_id,fini_product_id, prd_cost)
	SELECT   plant_id
	        ,[fini_product_id]
			,ROUND(prd_cost,2)AS prd_cost
	FROM #z
	
	-- Teste

	SELECT *
	FROM refine.trusted_prd_costs

ROLLBACK 


			