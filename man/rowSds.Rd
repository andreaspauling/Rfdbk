% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fdbk_asdataframe.R
\name{rowSds}
\alias{rowSds}
\title{Standard deviation on rows of array (faster than using 'apply')}
\usage{
rowSds(a, na.rm = F)
}
\arguments{
\item{a}{2d array}
}
\value{
standard deviation on rows
}
\description{
Standard deviation on rows of array (faster than using 'apply')
}
\examples{

a = array(rnorm(1e5),dim=c(1000,50))
system.time(rowSds(a))
system.time(apply(a,1,sd))
# Results agree besides some numerical precision errors
identical(round(rowSds(a),12),round(apply(a,1,sd),12))
}
\author{
Felix <felix.fundel@dwd.de>
}
