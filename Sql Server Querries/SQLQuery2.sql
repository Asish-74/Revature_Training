use RevCompanyDb
----FUNCTIONS -------
CREATE OR ALTER FUNCTION AvgEmpSal()
returns numeric(7,2)
as 
BEGIN 
	return  @avgsal
END