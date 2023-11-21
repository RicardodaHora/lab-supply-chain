
select top 10 * into #x from refine.trusted_customer


begin tran

select * into dbo.teste from #x

select * from  dbo.teste

rollback

