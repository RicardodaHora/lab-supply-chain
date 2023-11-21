BEGIN TRAN 

	--Criacao da tabela #x, utilizamos o SUM para retornar dados resumidos, junto do group by.

	DROP TABLE IF EXISTS #x

	SELECT   [date]
			,[fini_product_id]
			,[customer_id]
			,[country_id]
			,SUM([sales_qtd_kg])'sales_qtd_kg'
			,SUM([sales_reve_usd])'sales_reve_usd'
			,SUM([sales_cost_usd])'sales_cost_usd'

			INTO #x

		FROM [refine].[trusted_historical_sales]
		GROUP BY
			 [date]
			,[fini_product_id]
			,[customer_id]
			,[country_id]


	--Criacao da tabela #y, utilizamos o ROW_NUMBER 

	DROP TABLE IF EXISTS #y

	SELECT	 [fini_product_id]
			,[customer_id]
			,[country_id]
			,[sales_qtd_kg]
			,[sales_reve_usd]
			,ROW_NUMBER() OVER(PARTITION BY [fini_product_id],[customer_id],[country_id] ORDER BY [date] DESC) AS Row

			INTO #y

	FROM #x
	WHERE sales_qtd_kg <> 0


	--Criacao da tabela #z, utilizamos está tabela para fazer a operação matématica e criação da nova coluna (sell_price)

	DROP TABLE IF EXISTS #z

	SELECT	 [customer_id]
			,[country_id]
	        ,[fini_product_id]
			,AVG([sales_reve_usd]/[sales_qtd_kg]) AS sell_price
			
			INTO #z
	FROM #y
	WHERE [Row] BETWEEN 1 AND 5
	GROUP BY 
			 [customer_id]
			,[country_id]
	        ,[fini_product_id]


	--Criacao da tabela refine.trusted_seling_price

	CREATE TABLE refine.trusted_seling_price (customer_id int, country_id int, fini_product_id int, sell_price float)


	--Insercao na tabela principal 

	INSERT INTO refine.trusted_seling_price (customer_id, country_id, fini_product_id, sell_price)
	SELECT   [customer_id]
			,[country_id]
	        ,[fini_product_id]
			,ROUND([sell_price],2)AS sell_price
	FROM #z
	
	-- Teste
	SELECT *
	FROM refine.trusted_seling_price

ROLLBACK 




			
