BEGIN TRAN

  --Utilizei do recurso INTO #x para criar uma tabela temporária com as informações devidamente corrigidas e estruturadas,
  --informações essas reiradas da tabela base raw.RAW_FREIGHT
  --Foi utilizado o REPLACE para substituir os valores abreviados que estavam na tabela base pelos nomes contidos na coluna region da tabela refine.trusted_region, facilitando o relacionamento entre as tabelas.

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
    Mode_of_trans
INTO #FREIGHT 
FROM raw.RAW_FREIGHT;


CREATE TABLE refine.trusted_transfer_time (start_region_id int,
end_region_id int, transp_mode_id int, time_day int );
INSERT INTO refine.trusted_transfer_time (start_region_id, end_region_id, transp_mode_id, time_day)

SELECT TR.region_id AS start_region_id, TR2.region_id AS end_region_id, T.transp_mode_id,
CASE
WHEN T.transp_mode_id = 1 THEN 2 + ABS(CHECKSUM(NEWID())) % 27 -- Modo aéreo
WHEN T.transp_mode_id = 2 AND TR.region = TR2.region THEN 2 + ABS(CHECKSUM(NEWID())) % 13 -- Modo rodoviário (mesmas regiões)
WHEN T.transp_mode_id = 2 THEN 15 + ABS(CHECKSUM(NEWID())) % 30 -- Modo rodoviário (regiões diferentes)
WHEN T.transp_mode_id = 3 AND TR.region = TR2.region THEN 31 + ABS(CHECKSUM(NEWID())) % 29 -- Modo marítimo (mesmas regiões)
WHEN T.transp_mode_id = 3 THEN 45 + ABS(CHECKSUM(NEWID())) % 45 -- Modo marítimo (regiões diferentes)
END AS time_day

--EXPLICAÇAO DO CODIGO ACIMA
--Para cada linha de dados, o código começa verificando o modo de transporte (Modo aéreo, Modo rodoviário, ou Modo marítimo) com base no valor de T.transp_mode_id.
--Dependendo do modo de transporte, ele define um intervalo de valores que deseja gerar.
--Em seguida, utiliza a função CHECKSUM(NEWID()) para obter um número inteiro aleatório único para cada linha de dados. O CHECKSUM(NEWID()) gera uma "semente" de aleatoriedade única para cada linha.
--Aplica o operador % (módulo) a esse número aleatório. O operador % é usado para limitar o valor dentro do intervalo desejado. Isso garante que o valor gerado não ultrapasse os limites definidos.
--Finalmente, adiciona um valor de deslocamento apropriado (por exemplo, 2) para garantir que o valor gerado comece no limite inferior do intervalo desejado.

--EXPLIÇAO DA PARTE LOGICA
--O operador % (módulo) é usado para limitar o valor gerado pela função CHECKSUM(NEWID()) a um intervalo específico. Ele funciona assim:

--Suponha que CHECKSUM(NEWID()) gere um número aleatório, como 123456.
--Se aplicarmos o operador % a esse número, por exemplo, 123456 % 27, obteremos o restante da divisão de 123456 por 27.
--Isso resultará em um número entre 0 e 26 (o intervalo do módulo 27), e, em seguida, adicionamos 2 para garantir que o número gerado esteja no intervalo desejado (2 a 29).
--Em termos simples, o operador % ajuda a "encaixar" o número aleatório dentro do intervalo pretendido, limitando-o aos valores válidos.

--O valor de deslocamento é simplesmente adicionado ao resultado do cálculo do operador % para ajustar o valor gerado de modo que ele comece no limite inferior do intervalo desejado. Isso é feito para garantir que os números gerados estejam dentro dos intervalos definidos.

--Em resumo, o operador % é usado para garantir que o valor gerado esteja dentro do intervalo, e o valor de deslocamento é adicionado para ajustar esse valor para começar no limite inferior do intervalo desejado.
--Essa combinação de operações cria números aleatórios dentro dos intervalos especificados com base nas condições do transporte.


FROM #FREIGHT F
INNER JOIN [refine].[trusted_region] TR ON  F.ship_from_Region = TR.region
INNER JOIN [refine].[trusted_region] TR2 ON  F.shipto_Region = TR2.region
INNER JOIN [refine].[trusted_transp_mode] T ON F.Mode_of_trans=T.transp_mode

--SELECT para testar a tabela criada
SELECT *
FROM refine.trusted_transfer_time

  ROLLBACK

