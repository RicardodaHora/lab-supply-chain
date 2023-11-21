BEGIN TRAN

  --Utilizei do recurso INTO #x para criar uma tabela tempor�ria com as informa��es devidamente corrigidas e estruturadas,
  --informa��es essas reiradas da tabela base raw.RAW_FREIGHT
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
WHEN T.transp_mode_id = 1 THEN 2 + ABS(CHECKSUM(NEWID())) % 27 -- Modo a�reo
WHEN T.transp_mode_id = 2 AND TR.region = TR2.region THEN 2 + ABS(CHECKSUM(NEWID())) % 13 -- Modo rodovi�rio (mesmas regi�es)
WHEN T.transp_mode_id = 2 THEN 15 + ABS(CHECKSUM(NEWID())) % 30 -- Modo rodovi�rio (regi�es diferentes)
WHEN T.transp_mode_id = 3 AND TR.region = TR2.region THEN 31 + ABS(CHECKSUM(NEWID())) % 29 -- Modo mar�timo (mesmas regi�es)
WHEN T.transp_mode_id = 3 THEN 45 + ABS(CHECKSUM(NEWID())) % 45 -- Modo mar�timo (regi�es diferentes)
END AS time_day

--EXPLICA�AO DO CODIGO ACIMA
--Para cada linha de dados, o c�digo come�a verificando o modo de transporte (Modo a�reo, Modo rodovi�rio, ou Modo mar�timo) com base no valor de T.transp_mode_id.
--Dependendo do modo de transporte, ele define um intervalo de valores que deseja gerar.
--Em seguida, utiliza a fun��o CHECKSUM(NEWID()) para obter um n�mero inteiro aleat�rio �nico para cada linha de dados. O CHECKSUM(NEWID()) gera uma "semente" de aleatoriedade �nica para cada linha.
--Aplica o operador % (m�dulo) a esse n�mero aleat�rio. O operador % � usado para limitar o valor dentro do intervalo desejado. Isso garante que o valor gerado n�o ultrapasse os limites definidos.
--Finalmente, adiciona um valor de deslocamento apropriado (por exemplo, 2) para garantir que o valor gerado comece no limite inferior do intervalo desejado.

--EXPLI�AO DA PARTE LOGICA
--O operador % (m�dulo) � usado para limitar o valor gerado pela fun��o CHECKSUM(NEWID()) a um intervalo espec�fico. Ele funciona assim:

--Suponha que CHECKSUM(NEWID()) gere um n�mero aleat�rio, como 123456.
--Se aplicarmos o operador % a esse n�mero, por exemplo, 123456 % 27, obteremos o restante da divis�o de 123456 por 27.
--Isso resultar� em um n�mero entre 0 e 26 (o intervalo do m�dulo 27), e, em seguida, adicionamos 2 para garantir que o n�mero gerado esteja no intervalo desejado (2 a 29).
--Em termos simples, o operador % ajuda a "encaixar" o n�mero aleat�rio dentro do intervalo pretendido, limitando-o aos valores v�lidos.

--O valor de deslocamento � simplesmente adicionado ao resultado do c�lculo do operador % para ajustar o valor gerado de modo que ele comece no limite inferior do intervalo desejado. Isso � feito para garantir que os n�meros gerados estejam dentro dos intervalos definidos.

--Em resumo, o operador % � usado para garantir que o valor gerado esteja dentro do intervalo, e o valor de deslocamento � adicionado para ajustar esse valor para come�ar no limite inferior do intervalo desejado.
--Essa combina��o de opera��es cria n�meros aleat�rios dentro dos intervalos especificados com base nas condi��es do transporte.


FROM #FREIGHT F
INNER JOIN [refine].[trusted_region] TR ON  F.ship_from_Region = TR.region
INNER JOIN [refine].[trusted_region] TR2 ON  F.shipto_Region = TR2.region
INNER JOIN [refine].[trusted_transp_mode] T ON F.Mode_of_trans=T.transp_mode

--SELECT para testar a tabela criada
SELECT *
FROM refine.trusted_transfer_time

  ROLLBACK

