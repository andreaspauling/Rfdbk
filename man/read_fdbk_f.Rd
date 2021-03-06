% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fdbk_asdataframe.R
\name{read_fdbk_f}
\alias{read_fdbk_f}
\title{Load the entire content of a fdbk file or only some specified variables (faster and more resource friendly)}
\usage{
read_fdbk_f(filename, vars = "")
}
\arguments{
\item{filename}{NetCDF fdbk filename including path}

\item{vars}{vector of variables that should be retained if not specified or "" all variables are loaded}
}
\value{
a list of entries from the given fdbk file
}
\description{
Load the entire content of a fdbk file or only some specified variables (faster and more resource friendly)
}
\examples{
fdbk = read_fdbk_f("~/examplesRfdbk/icon/synop/verSYNOP.2014120112",c("obs","veri_data"))
str(fdbk)
}
\author{
Felix <felix.fundel@dwd.de>
}
