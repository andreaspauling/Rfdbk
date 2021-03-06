% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fdbk_asdataframe.R
\name{fdbk_dt_uv2spd}
\alias{fdbk_dt_uv2spd}
\title{Calculate wind speed from u and v wind components in a data.table}
\usage{
fdbk_dt_uv2spd(DATATABLE, col = c("obs", "veri_data"))
}
\arguments{
\item{DATATABLE}{data table containing the columns "varno" with elements 3 and 4, and e.g. "obs", "obs_ini", "veri_data" or combinations of it}

\item{fcst}{forecast vector}
}
\value{
data.table with same columns as DATATABLE and varno=112
}
\description{
Calculate wind speed from u and v wind components in a data.table
}
\examples{
fnames   = system("ls ~/examplesRfdbk/icon/synop/*",intern=T)[1:5]
cond     = list(obs                = "!is.na(obs)",
                veri_run_class     = "veri_run_class\%in\%c(0,2)",
                veri_run_type      = "veri_run_type\%in\%c(0,4)",
                state              = "state\%in\%c(0,1,5)",
                statid             = "!is.na(as.numeric(statid))",
                statid             = "!duplicated(statid)",
                varno              = "varno\%in\%c(3,4)")
colnames  = c("obs","veri_data","veri_forecast_time","veri_initial_date","lat","lon","varno","veri_model","statid","z_station")
DT        = fdbk_dt_multi_large(fnames,cond,colnames,cores=5)
SPD       = fdbk_dt_uv2spd(DT)  
.rbind.data.table(DT,SPD)
}
\author{
Felix <felix.fundel@dwd.de>
}
