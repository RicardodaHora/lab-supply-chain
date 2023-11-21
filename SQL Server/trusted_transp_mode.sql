BEGIN TRAN

CREATE TABLE refine.trusted_transp_mode (transp_mode_id int primary key identity (1,1), transp_mode varchar(10) NOT NULL);
INSERT INTO	refine.trusted_transp_mode (transp_mode)
SELECT DISTINCT Mode_of_trans FROM [raw].[RAW_FREIGHT]

SELECT *
FROM refine.trusted_transp_mode

ROLLBACK


