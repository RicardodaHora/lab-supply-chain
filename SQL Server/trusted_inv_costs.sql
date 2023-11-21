BEGIN TRAN

		CREATE TABLE refine.trusted_inv_costs (plant_id INT, inv_cost FLOAT,
		FOREIGN KEY (plant_id) REFERENCES [refine].[trusted_plant_id]([plant_id]));

		INSERT INTO refine.trusted_inv_costs (plant_id, inv_cost)
		SELECT p.[plant_id], 
			   CASE 
				   WHEN tp.is_prod = 1 THEN 3.00 + ABS(CHECKSUM(NEWID())) % 5.00 -- Planta de producao
				   WHEN tp.is_fini = 1 THEN 1.50 + ABS(CHECKSUM(NEWID())) % 3.00 -- Planta de finalizacao
				   WHEN tp.is_wh   = 1 THEN 0.50 + ABS(CHECKSUM(NEWID())) % 1.00 -- Planta de Warehouses
				   ELSE 0.0 -- Outros tipos de planta (se houver)
			   END AS inv_cost
		FROM [refine].[trusted_plant_id] p
		INNER JOIN [refine].[trusted_plant] tp ON p.[plant_id] = tp.[plant_id];

		
		SELECT *
		FROM refine.trusted_inv_costs

ROLLBACK 