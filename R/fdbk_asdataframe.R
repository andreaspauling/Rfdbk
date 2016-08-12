#' Function to sort a given date to meteorological seasons (DJF, MAM, JJA, SON).
#' Useful to stratify scores by season in order to plot scores for different seasons
#' and compare them
#' @param a date in format yyyymmdd (at least). hours and/or minutes and/or seconds can be
#' specified in format yyyymmddHHMMSS. Can be given as a string or numeric.
#'
#' @return a string corresponding to the four seasons (DJF, MAM, JJA, SON)
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE 1 simple examples with one date
#' asSeason("20150201") returns "DJF"
#' asSeason(20150201) also returns "DJF"
#' asSeason("201502011234") also returns "DJF"
#' asSeason("151201") returns an error (format yymmdd not accepted)
#' #EXAMPLE 2 Example of how to use this function to stratify scores by season
#' and show a plot of comparison fot different seasons
#'
#' require(ggplot2)
#' fnames       = "/Users/josuegehring/Desktop/verTEMP.2014120112"
#' cond        = list(obs="!is.na(obs)",varno="varno%in%c(2,3,4,29)",ident="ident%in%c(6610)",varno="varno%in%c(2)")
#' columnnames = c("obs","veri_data","varno","state","level","veri_initial_date","ident")
#' DT          = fdbk_dt_multi_large(fnames,cond,columnnames,1)
#' levels = c(100000,92500,85000,70000,60000,50000,40000,30000)
#' DT = fdbk_dt_binning_level(DT,"level",levels,includeAll=TRUE)
#' DT$varno    = varno_to_name(DT$varno)
#' DT$season = as.character(lapply(DT$veri_initial_date, asSeason))
#' DT = na.omit(DT)
#' strat       = c("season","level")
#' scores      = fdbk_dt_verif_continuous(DT,strat)
#' scores      = scores[!is.na(scores),]
#' ii = scores$scorename=="ME"
#' scores = scores[ii]
#' data = data.frame(scores$level,scores$scores,scores$season)
#' colnames(data) = c("level","scores","season")
#' data = data[order(data$level),]
#' p =  ggplot(data,aes(x=scores,y=level,group=season,colour=season))+
#'   geom_point()+geom_path() + 
#'   theme_bw()+theme(axis.text.x  = element_text(angle=70,hjust = 1))+scale_y_reverse()
#' print(p)
#' 
asSeason <- function(x) {
  date = as.Date(as.character(x), format = "%Y%m%d")
  m = as.numeric(format(date, "%m"))
  
  if (m >= 12 | m <= 2) {
    return("DJF")
  }
  else if (m >= 3 & m <= 5) {
    return("MAM")
  }
  else if (m >= 6 & m <= 8) {
    return("JJA")
  }
  else if (m >= 9 & m <= 11) {
    return("SON")
  }
  else {
    return("NA")
  }
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Function to convert time in format hhmm to decimal hours. 
#' Useful to calculate a derived time from two time informations
#' @param time in format hhmm. Can be a string or numeric
#' @return time in decimal hours
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE 1 simple examples
#' hhmm2hour(0145) returns 1.75
#' hhmm2hour(145) returns 1.75
#' hhmm2hour("145") returns 1.75
#' 
#' #EXAMPLE 2 calculate leadtime from veri_forecast_time and time
#' require(ggplot2)
#' fnames       = "/Users/josuegehring/Desktop/verTEMP.2014120112"
#' cond        = list(obs="!is.na(obs)",varno="varno%in%c(2,3,4,29)",ident="ident%in%c(6610)",varno="varno%in%c(2)")
#' columnnames = c("obs","veri_data","varno","state","level","veri_forecast_time","time","ident")
#' DT          = fdbk_dt_multi_large(fnames,cond,columnnames,1)
#' leadtime = hhmm2hour(DT$veri_forecast_time) + DT$time/60
#' DT[,"leadtime"] = leadtime
#' 
hhmm2hour = function(x){
  x = as.numeric(x)
  hours <- trunc(x / 100)
  mins <- 100 * (x/100 - hours)
  return(hours + mins/60)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Function to load one or many fdbk Files and transform them to a data.table.
#' Faster than fdbk_dt_multi and able to handle very large files, however,
#' be as restrictive as possible, use the cond/columnnames argument select only the data you need for your problem.
#' Note: Using conditions on veri_data in the cond argument is not possible and may cause an error!!!
#' Solution: filter veri_data in the returned data.table
#'
#' @param fnames      vector of feedback filename(s)
#' @param cond        list of strings of conditions (all of the list entries are connected with the "&" operator!)
#' @param columnnames attribute names to keep in the data table
#' @param cores       use multiple cores for parallel file loading
#'
#' @return a data.table of merged feedback file contents
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE 1 (1x1 deg.) bias of satellite data (channel 921 from METOP-1)
#' require(ggplot2)
#' fnames      = system("/bin/ls ~/examplesRfdbk/example_monRad/monRAD_*.nc",intern=T)
#' condition   = list(obs="!is.na(obs)",
#'                    level="level%in%c(921)",
#'                    statid="statid=='METOP-1   '",
#'                    veri_forecast_time="veri_forecast_time==0",
#'                    veri_run_type="veri_run_type==3",
#'                    veri_ens_member="veri_ens_member==-1")
#' columnnames = c("obs","veri_data","lon","lat","veri_initial_date")
#' DT          = fdbk_dt_multi_large(fnames,condition,columnnames,cores=1)
#' DT
#' DT[,lon:=round(lon)]
#' DT[,lat:=round(lat)]
#' scores = DT[,list(ME=mean(obs-veri_data)),by=c("lon","lat")]
#' outlines = as.data.table(map("world", plot = FALSE)[c("x","y")])
#' worldmap = geom_path(aes(x, y), inherit.aes = FALSE, data = outlines, alpha = 0.8, show_guide = FALSE,size = .2)
#' p = ggplot(scores,aes(x=lon,y=lat,fill=cut(ME,seq(-100,100,20))))+geom_raster()+
#'     scale_fill_manual("ME",values=tim.colors(10),drop = FALSE)+
#'     worldmap
#' p
#' 
#'  #EXAMPLE 2 TEMP EPS plot for one station on reversed-log-y scale
#' require(ggplot2)
#' require(scales)
#' fname="~/examplesRfdbk/eps/2013111112/verTEMP.nc"
#' condition           = list(veri_description="grepl('first guess vv',veri_description)",
#'                            veri_description="grepl('member',veri_description)",
#'                            state="state%in%c(0,1)",
#'                            statid="statid=='01028     '")
#' columns             = c("obs","veri_data","varno","level","veri_description","veri_forecast_time","statid")
#' DT                  = fdbk_dt_multi_large(fname,condition,columns,1)
#' DT$veri_description = as.numeric(substr(DT$veri_description,29,32))
#' setnames(DT,"veri_description","member")
#' DT[,varno:=varno_to_name(varno,F)]
#' reverselog_trans <- function(base = exp(1)) {
#'      trans <- function(x) -log(x, base)
#'      inv <- function(x) base^(-x)
#'      trans_new(paste0("reverselog-", format(base)), trans, inv, 
#'                log_breaks(base = base), 
#'                domain = c(1e-100, Inf))
#' }
#' 
#' # plot only even members for clearness+ obs as black line
#' ggplot(DT[DT$member%%2==0,],aes(x=veri_data,y=level,color=factor(member)))+geom_path()+geom_point()+facet_wrap(~varno,scale="free_x")+
#'       scale_y_continuous(trans=reverselog_trans(10))+
#'       geom_point(data =DT[member==1], aes(x=obs,y=level), colour = "black")+
#'       geom_path(data =DT[member==1], aes(x=obs,y=level), colour = "black")+
#'       ggtitle(paste("EPS TEMP for station",unique(DT$statid)))
#'
#'  #EXAMPLE 3 SATELLITE RADIATION plot verification scores as function of channel and staellite
#' require(ggplot2)
#' fnames      = system("ls ~/examplesRfdbk/example_monRad/monRAD_*.nc",intern=T)
#' condition   = list(obs="!is.na(obs)",
#'                    level="level>100 & level<6000",
#'                    veri_forecast_time="veri_forecast_time==0",
#'                    veri_run_type="veri_run_type==3",
#'                    veri_ens_member="veri_ens_member==-1")
#' DT         = fdbk_dt_multi_large(fnames,condition,c("obs","veri_data","level","statid"),1)
#' scores     = fdbk_dt_verif_continuous(DT,c("level","statid")) 
#' ggplot(scores,aes(x=level,y=scores,color=statid,group=statid))+geom_line()+geom_point()+facet_wrap(~scorename,scale="free")
fdbk_dt_multi_large <- function(fnames,condition="",vars="",cores=1){
  loadt <- function(fname,condition,vars){
    varsinner = unique(c(vars,"i_body","l_body",names(condition)))
    if (any(grepl("radar_",vars))) varsinner = unique(c(varsinner,"radar_nbody"))
    DT  = fdbk_dt(read_fdbk_large(fname,condition,vars=varsinner))
    if (all(vars!="")) DT  = DT[,vars,with=F]
    print(paste("completed reading",fname))
    return(DT)
  }
  return(do.call(.rbind.data.table,mclapply(fnames,loadt,condition,vars,mc.cores = cores)))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Load one fdbk file and return as list of lists of....
#' condition and vars arguments help to discard data you do not need
#'
#' @param fname       feedback filename (including path)
#' @param condition   list of strings of conditions (all of the list entries are connected with the "&" operator!)
#' @param vars        vector of variable names that should be retained if not specified or "" all variables are loaded
#'
#' @return a data.table with fdbk file content
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE 1 (1x1 deg.) bias of satellite data (channel 921 from METOP-1)
#' fnames      = "~/examplesRfdbk/example_monRad/monRAD_2014092406.nc"
#' condition   = list(obs="!is.na(obs)",
#'                    level="level%in%c(921)",
#'                    statid="statid=='METOP-1   '",
#'                    veri_forecast_time="veri_forecast_time==0",
#'                    veri_run_type="veri_run_type==3",
#'                    veri_ens_member="veri_ens_member==-1")
#' fdbk         = read_fdbk_large(fnames,condition,c("lon","lat","obs"))
#' x11(width=12,height=7.5)
#' scatterplot(fdbk$DATA$lon$values,fdbk$DATA$lat$values,fdbk$DATA$obs$values,pch=20,cex=.5,cpal=tim.colors(),ncol=30);world(add=T)
read_fdbk_large <- function(fname,condition="",vars=""){

  # read feedback file as list (entire file without redundancy)
  if (any(vars=="")){
    fdbk = read_fdbk_f(fname,vars="")
  }else{  
    fdbk = read_fdbk_f(fname,vars=unique(c(vars,"i_body","l_body",names(condition))))
  }
  
  # if no filter is set return file as is
  if (condition[1]==""){return(fdbk)}
  
  # if any NA in hdr remove them as well
  if (any(is.na(fdbk$DATA$i_body$values))){
    condition = append(condition,list(i_body="!is.na(i_body)"))
    condition = condition[!duplicated(condition)]
  }
  
  #  dimensions
  d_veri     = fdbk$DIMENSIONS$d_veri$length
  d_hdr      = fdbk$DIMENSIONS$d_hdr$length
  d_body     = fdbk$DIMENSIONS$d_body$length
  dimensions = lapply(lapply(fdbk$DATA,"[[","values"),dim)
  
  
  # variable names for each type of date
  names_body  = names(which(unlist(lapply(lapply(fdbk$DATA,"[[","dimids"),"[",1))==fdbk$DIMENSIONS$d_body$id))
  names_hdr   = names(which(unlist(lapply(lapply(fdbk$DATA,"[[","dimids"),last))==fdbk$DIMENSIONS$d_hdr$id))
  names_veri  = names(which(unlist(lapply(lapply(fdbk$DATA,"[[","dimids"),last))==fdbk$DIMENSIONS$d_veri$id))
  names_body  = names_body[!names_body%in%names_veri]
  
  type_table = data.table(NULL)
  if (length(names_body)>0){ type_table = rbind(type_table,data.table(names=names_body,type="body"))}
  if (length(names_hdr)>0){  type_table = rbind(type_table,data.table(names=names_hdr,type="hdr"))}
  if (length(names_veri)>0){ type_table = rbind(type_table,data.table(names=names_veri,type="veri"))}
  
  # filter types that have to be applied
  filter_type = type_table[match(names(condition),type_table$names),type]
  
  
  # "VERI" FILTER
  if (any(filter_type=="veri")){
    filt_cond  = paste(unlist(lapply(condition[which(filter_type=="veri")],"[")),collapse=" & ")
    filt_var   = names(lapply(condition[which(filter_type=="veri")],"["))
    for (f in unique(filt_var)){
      eval(parse(text=paste0(f,"=fdbk$DATA[[f]]$values"))) # assign values to variable
    }
    filt = eval(parse(text=filt_cond))  # filter condition
    if (all(!filt)){
      warning(paste(filt_cond,"has no match, returning NULL"))
      return(NULL)
    }
    #for (i in which(unlist(lapply(lapply(dimensions,"[",1),"==",d_veri)))){
    #  fdbk$DATA[[i]]$values =  fdbk$DATA[[i]]$values[filt] # filter 1d values
    #}
    #for (i in which(unlist(lapply(lapply(dimensions,"[",2),"==",d_veri)))){
    #  fdbk$DATA[[i]]$values =  matrix(fdbk$DATA[[i]]$values[,filt],ncol=sum(filt)) # filter 2d values
    #}
    for (i in names(dimensions)[names(dimensions)%in%names_veri]){
      if(length(dim(fdbk$DATA[[i]]$values))==1){
        fdbk$DATA[[i]]$values =  fdbk$DATA[[i]]$values[filt] # filter 1d values
      }else{
        fdbk$DATA[[i]]$values =  matrix(fdbk$DATA[[i]]$values[,filt],ncol=sum(filt)) # filter 2d values
      }
    }


    fdbk$DIMENSIONS$d_veri$length = sum(filt) # correct fdbk$DIMENSIONS entry
  } 
  
  # "REPORT" FILTER
  if (any(filter_type=="hdr")){
    filt_cond  = paste(unlist(lapply(condition[which(filter_type=="hdr")],"[")),collapse=" & ")
    filt_var   = names(lapply(condition[which(filter_type=="hdr")],"["))
    for (f in filt_var){
      eval(parse(text=paste0(f,"=fdbk$DATA[[f]]$values"))) # assign values to variable
    }
    filt = which(eval(parse(text=filt_cond)))
    if (length(filt)==0){
      warning(paste(filt_cond,"has no match, returning NULL"))
      return(NULL)
    }
    # filter body length variables
    indices = do.call("c",mapply(seq,fdbk$DATA$i_body$values[filt],fdbk$DATA$i_body$values[filt]+fdbk$DATA$l_body$values[filt]-1,SIMPLIFY = F))
    for (i in which(unlist(lapply(lapply(dimensions,"[",1),"==",d_body)))){
      if (length(dim(fdbk$DATA[[i]]$values))==2){
        fdbk$DATA[[i]]$values =  matrix(fdbk$DATA[[i]]$values[indices,],ncol=dim(fdbk$DATA[[i]]$values)[2])
      }else{
        fdbk$DATA[[i]]$values =  fdbk$DATA[[i]]$values[indices]
      }
    }
    # filter report length variables
    for (i in which(unlist(lapply(lapply(dimensions,"[",1),"==",d_hdr)))){
      fdbk$DATA[[i]]$values =  fdbk$DATA[[i]]$values[filt]
    }
    fdbk$DIMENSIONS$d_hdr$length = length(filt) # correct fdbk$DIMENSIONS entry
    fdbk$DIMENSIONS$d_body$length  = length(fdbk$DATA$obs$values)
    fdbk$DATA$i_body$values      = cumsum(c(1,fdbk$DATA$l_body$values))[-(length(fdbk$DATA$l_body$values)+1)] # correct fdbk$DATA$i_body entries
  }
  
  
  
  # "BODY" FILTER
  if (any(filter_type=="body")){
    filt_cond  = paste(unlist(lapply(condition[which(filter_type=="body")],"[")),collapse=" & ")
    filt_var   = names(lapply(condition[which(filter_type=="body")],"["))
    indices    = rep(seq(1,length(fdbk$DATA$i_body$values)),fdbk$DATA$l_body$values)
    for (f in filt_var){
      eval(parse(text=paste0(f,"=fdbk$DATA[[f]]$values"))) # assign values to variable
    }
    filt = which(eval(parse(text=filt_cond)))
    if (length(filt)==0){
      warning(paste(filt_cond,"has no match, returning NULL"))
      return(NULL)
    }
    # filter body length variables
    for (i in which(unlist(lapply(lapply(dimensions,"[",1),"==",d_body)))){
      if (length(dim(fdbk$DATA[[i]]$values))==2){
        fdbk$DATA[[i]]$values =  matrix(fdbk$DATA[[i]]$values[filt,],ncol=dim(fdbk$DATA[[i]]$values)[2])
      }else{
        fdbk$DATA[[i]]$values =  fdbk$DATA[[i]]$values[filt]
      }
    }
    # filter report where new_l_body==0
    filt_hdr = !unique(indices)%in%unique(indices[filt])
    for (i in which(unlist(lapply(lapply(dimensions,"[",1),"==",d_hdr)))){
      fdbk$DATA[[i]]$values =  fdbk$DATA[[i]]$values[!filt_hdr]
    }
    # delet rows new_l_body==0
    fdbk$DATA$l_body$values = as.integer(table(indices[filt]))
    fdbk$DATA$i_body$values = cumsum(c(1,fdbk$DATA$l_body$values))[-(length(fdbk$DATA$l_body$values)+1)]
    fdbk$DIMENSIONS$d_hdr$length  = length(fdbk$DATA$l_body$values)
    fdbk$DIMENSIONS$d_body$length  = length(fdbk$DATA$obs$values)
  }
  
  # change global information
  fdbk$GLOBALS$n_body = fdbk$DIMENSIONS$d_body$length
  fdbk$GLOBALS$n_hdr  = fdbk$DIMENSIONS$d_hdr$length
  
  # delete variables that are not wanted
  if (any(vars!="")){
    fdbk$DATA = fdbk$DATA[names(fdbk$DATA)[match(unique(c(vars,"i_body","l_body")),names(fdbk$DATA))]]
  }
  
  return(fdbk)
  
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Fdbk file content (as obtained from read_fdbk(_f)) is converted into a data.table.
#' Therefore a lot of data overhead is created as most data will be duplicated.
#' However, data.tables offer a lot of extra functionality.
#'
#' @param fdbk output from read_fdbk
#' @return a data.table of the feedback file data section
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' fdbk = read_fdbk("~/examplesRfdbk/icon/synop/verSYNOP.2014120112")
#' format(object.size(fdbk),"Mb")
#' DT   = fdbk_dt(fdbk)
#' format(object.size(DT),"Mb")
#' DT
fdbk_dt <- function(fdbk){
	if (is.null(fdbk)) {
		return(NULL)
	}
	data_len     = fdbk$DIMENSIONS$d_body$length
	veri_steps   = fdbk$DIMENSIONS$d_veri$length
	stat_len     = fdbk$DIMENSIONS$d_hdr$length
	radar_len    = fdbk$DIMENSIONS$d_radar$length
	var_lengths  = lapply(lapply(fdbk$DATA, "[[", "values"), length)
	data_names   = names(var_lengths)
	obs_id       = as.vector(which(unlist(lapply(var_lengths, "==",data_len))))
	obs_names    = data_names[obs_id]
	veri_id      = as.vector(which(unlist(lapply(var_lengths, "==",veri_steps))))
	veri_names   = data_names[veri_id]
	stat_id      = as.vector(which(unlist(lapply(var_lengths, "==",stat_len))))
	stat_names   = data_names[stat_id]
	radar_id     = as.vector(which(unlist(lapply(var_lengths, "==",radar_len))))
	radar_names   = data_names[radar_id][grepl("radar_",data_names[radar_id])]

	dlist        = list()
	for (n in obs_names) {
		dlist[[n]] = rep(fdbk$DATA[[n]]$values[1:fdbk$GLOBALS$n_body],veri_steps)
	}
	for (n in veri_names) {
		dlist[[n]] = rep(fdbk$DATA[[n]]$values, each = fdbk$GLOBALS$n_body)
	}
	for (n in stat_names) {
		dlist[[n]] = rep(rep(fdbk$DATA[[n]]$values[1:fdbk$GLOBALS$n_hdr],fdbk$DATA$l_body$values[1:fdbk$GLOBALS$n_hdr]), veri_steps)
	}
	for (n in radar_names) {
		dlist[[n]] = c()
		for (i in 1:fdbk$GLOBALS$n_hdr){
			radfrom    = last(which(cumsum(fdbk$DATA$radar_nbody$values)==fdbk$DATA$i_body$values[i]-1))
			radto      = which(cumsum(fdbk$DATA$radar_nbody$values)==cumsum(fdbk$DATA$l_body$values)[i])[1]
			dlist[[n]] = c(dlist[[n]],rep(fdbk$DATA[[n]]$values[radfrom:radto],fdbk$DATA$radar_nbody$values[radfrom:radto]))
		}
		dlist[[n]] = rep(dlist[[n]],veri_steps)
	}
	if (veri_steps > 1 & fdbk$GLOBALS$n_body > 1) {
		dlist[["veri_data"]] = as.vector(fdbk$DATA[["veri_data"]]$values[1:fdbk$GLOBALS$n_body,])
	}else if (veri_steps == 1 & fdbk$GLOBALS$n_body > 1) {
		dlist[["veri_data"]] = as.vector(fdbk$DATA[["veri_data"]]$values[1:fdbk$GLOBALS$n_body])
	}else if (veri_steps > 0 & fdbk$GLOBALS$n_body == 1) {
		dlist[["veri_data"]] = as.vector(fdbk$DATA[["veri_data"]]$values)
	}
	setDT(dlist)
	return(dlist)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Load the entire content of a fdbk file
#' 
#' @param filename NetCDF fdbk filename including path
#' 
#' @return a list of entries from the given fdbk file
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' fdbk = read_fdbk("~/examplesRfdbk/icon/synop/verSYNOP.2014120112")
#' str(fdbk)
read_fdbk <- function(filename){
  f              = open.nc(filename)
  n_global_attr  = file.inq.nc(f)$ngatts
  g_names        = c(); for (i in 0:(n_global_attr-1)) g_names=c(g_names,att.inq.nc(f,"NC_GLOBAL",i)$name)
  GLOBALS        = list(); for (i in 0:(n_global_attr-1)) GLOBALS[[i+1]] = att.get.nc(f,"NC_GLOBAL",i)
  names(GLOBALS) = g_names
  
  DATA     = list()
  for (i in 0:(file.inq.nc(f)$nvars-1)){
    DATA[[i+1]]           = var.inq.nc(f,i)
    for (j in 0:(DATA[[i+1]]$natts-1)){
      attname = att.inq.nc(f, DATA[[i+1]]$name, j)$name
      eval(parse(text=paste("DATA[[i+1]]$`",attname,"`  = att.get.nc(f,DATA[[i+1]]$name,'",attname,"')",sep="")))
    }
    DATA[[i+1]]$values    = var.get.nc(f,DATA[[i+1]]$name)
  }
  
  short       = do.call("rbind", lapply(DATA, "[[", 2))
  names(DATA) = short
  
  DIMENSIONS = list(); for (i in 0:(file.inq.nc(f)$ndims-1)){ DIMENSIONS[[i+1]] = dim.inq.nc(f,i) }
  names(DIMENSIONS) = lapply(DIMENSIONS,"[[","name")
  
  close.nc(f)
  return(list(DIMENSIONS=DIMENSIONS,GLOBALS=GLOBALS,DATA=DATA))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################


#' Load the entire content of a fdbk file or only some specified variables (faster and more resource friendly)
#' 
#' @param filename NetCDF fdbk filename including path
#' @param vars vector of variables that should be retained if not specified or "" all variables are loaded
#' 
#' @return a list of entries from the given fdbk file
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' fdbk = read_fdbk_f("~/examplesRfdbk/icon/synop/verSYNOP.2014120112",c("obs","veri_data"))
#' str(fdbk)
read_fdbk_f <- function(filename,vars=""){
  f              = open.nc(filename)
  n_global_attr  = file.inq.nc(f)$ngatts
  g_names        = c(); for (i in 0:(n_global_attr-1)) g_names=c(g_names,att.inq.nc(f,"NC_GLOBAL",i)$name)
  GLOBALS        = list(); for (i in 0:(n_global_attr-1)) GLOBALS[[i+1]] = att.get.nc(f,"NC_GLOBAL",i)
  names(GLOBALS) = g_names
  
  DATA     = list()
  for (i in 0:(file.inq.nc(f)$nvars-1)){
    DATA[[i+1]]           = var.inq.nc(f,i)
    if (DATA[[i+1]]$name%in%vars | all(vars=="")){
      for (j in 0:(DATA[[i+1]]$natts-1)){
        attname = att.inq.nc(f, DATA[[i+1]]$name, j)$name
        eval(parse(text=paste("DATA[[i+1]]$`",attname,"`  = att.get.nc(f,DATA[[i+1]]$name,'",attname,"')",sep="")))
      }
      DATA[[i+1]]$values    = var.get.nc(f,DATA[[i+1]]$name)
    }
  }
  
  short       = do.call("rbind", lapply(DATA, "[[", 2))
  names(DATA) = short
  if (any(vars!="")){
    DATA = DATA[short[match(vars,short)]]
  }
  
  DIMENSIONS = list(); for (i in 0:(file.inq.nc(f)$ndims-1)){ DIMENSIONS[[i+1]] = dim.inq.nc(f,i) }
  names(DIMENSIONS) = lapply(DIMENSIONS,"[[","name")
  
  close.nc(f)
  return(list(DIMENSIONS=DIMENSIONS,GLOBALS=GLOBALS,DATA=DATA))
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Load relevant information of many feedback files as data.table
#' Be restrictive with the columns kept in the data.table as otherwise 
#' the memory limit is  reached fast
#' To speed up computation multiple cores are utilized (if possible)
#'
#' @param fnames  vector of feedback filename(s)
#' @param cond    string of conditions the fdbk file will be filtered for in advance
#' @param columnnames attribute names to keep in the data table
#'
#' @return a data.table of merged feedback file contents
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' fnames      = system("ls ~/examplesRfdbk/icon/synop/verSYNOP.*",intern=T)
#' cond        = "varno%in%c(3,4) & !is.na(obs)"
#' columnnames = c("obs","veri_data","varno","veri_forecast_time")
#' DT          = fdbk_dt_multi(fnames,cond,columnnames,4)
#' DT
fdbk_dt_multi <- function(fnames,cond="",columnnames="",cores=1){
  print("INFO: Function is deprecated! Use fdbk_dt_multi_large for large files and better performance")
  loadt <- function(fname,cond,columnnames){
    DT  = fdbk_dt(read_fdbk(fname))
    if (cond!="")             DT  = droplevels(eval(parse(text=paste("DT[",cond,",,]",sep=""))))
    if (all(columnnames!="")) DT  = DT[,columnnames,with=F]
    print(paste("completed reading",fname))
    return(DT)
  }
  return(do.call(.rbind.data.table,mclapply(fnames,loadt,cond,columnnames,mc.cores = cores)))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################


#' Deterministic scores for data.tables from feedback files, returns 5-95 confidence intervals if needed.
#'
#' Function returns a score data.table with ME,MAE,RMSE,SD,R2 and  length of verification data pairs
#' Additionaly 5th and 95th confidence interval from bootstrap resampling can be returned.
#' ( Do not use to verify e.g. wind direction or similarly strange data types (as ordinary differences make no sense))
#'
#' @param DT the data table (obs and veri_data are required)
#' @param strat list of variables to stratify for
#' @param bootscores logical if bootstrap confidence intervals are required (5-95)
#' @param R number of bootstrap iterations (default 100)
#'
#' @return a data.table of stratified continuous verification scores  (ME,SD,RMSE,R2,LEN)(CI_L,CI_U if bootstrap)
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#'
#' #EXAMPLE 1 (continuous scores by lead-time)
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/*",intern=T)
#' cond                  = list(varno="varno%in%c(3,4)",veri_description="grepl('forecast',veri_description)")
#' columnnames           = c("obs","veri_data","varno","veri_model","veri_forecast_time")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' DT$varno              = varno_to_name(DT$varno)
#' strat                 = c("varno","veri_forecast_time","veri_model")
#' scores                = fdbk_dt_verif_continuous(DT,strat)
#' p =  ggplot(scores,aes(x=veri_forecast_time,y=scores,group=interaction(scorename,varno,veri_model),colour = veri_model, linetype=factor(varno)))+
#'      geom_line(size=.7) + geom_point(size=1.5) + facet_wrap(~scorename, scales = "free")+
#'      theme_bw()+theme(axis.text.x  = element_text(angle=70,hjust = 1))
#' p
#'
#' #EXAMPLE 2 (talagrand diagram for each variable)
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/talagrand/*SYNOP*",intern=T)
#' cond                  = list(veri_description="grepl('Talagrand',veri_description)")
#' columnnames           = c("veri_data","varno")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' DT$varno              = varno_to_name(DT$varno)
#' p                     = ggplot(DT, aes(x=veri_data)) + 
#'                         geom_histogram(binwidth=1, colour="black", fill="white") + 
#'                         facet_wrap(~varno)+theme_bw()
#' p
#' 
#' #EXAMPLE 3 (TEMP verification)
#' require(ggplot2)
#' fnames=system("ls ~/examplesRfdbk/fof/*", intern=T)
#' cond = list(obs="!is.na(obs)",level="level%in%c(100000,92500,85000,70000,50000,40000,30000,25000,20000,15000,10000,5000,3000,2000,1000)")
#' columnnames = c("obs","veri_data","varno","level")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,cores=20)
#' DT$varno              = varno_to_name(DT$varno)
#' strat                 = c("varno","level")
#' scores                = fdbk_dt_verif_continuous(DT,strat)
#' setkey(scores, scorename,varno,level)
#' scores                = scores[!scorename%chin%c("LEN"),]
#' p =  ggplot(scores,aes(x=scores,y=level,group=interaction(varno,scorename)))+
#'      geom_path() + facet_wrap(~scorename~varno,scales="free_x")+
#'      theme_bw()+theme(axis.text.x  = element_text(angle=70,hjust = 1))+scale_y_reverse()
#' p
#'
#' #EXAMPLE 4 (SATOB verification)
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/gme/satob/*",intern=T)
#' cond                  = list(obs="!is.na(obs)")
#' columnnames           = c("veri_data","varno","obs","veri_forecast_time","statid","lat","lon")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,10)
#' DT[,lon:=cut(lon,seq(-180,180,by=10),labels=seq(-175,175,by=10),include.lowest=T),]
#' DT[,lat:=cut(lat,seq(-90,90,by=10),labels=seq(-85,85,by=10),include.lowest=T),]
#' strat                 = c("varno","veri_forecast_time","statid","lon","lat")
#' scores                = fdbk_dt_verif_continuous(DT,strat)
#' scores[,lon:=as.numeric(levels(lon))[lon]]
#' scores[,lat:=as.numeric(levels(lat))[lat]]
#' scores[,varno:=varno_to_name(varno)]
#' scores                = scores[!is.na(scores),]
#' p = ggplot(droplevels(scores[varno=="U" & veri_forecast_time=="10800" & scorename=="R2", ]),aes(x=lon,y=lat,fill=cut(scores,seq(0,1,by=.1))))+geom_raster()+
#'     facet_wrap(~varno~statid~scorename)+
#'     scale_fill_manual(breaks=seq(0,1,by=.1),values=tim.colors(10),drop = FALSE)+borders()
#' p
#'
#' #EXAMPLE 5 (SYNOP score time series)
#' require(ggplot2)
#' fnames   = system("ls ~/examplesRfdbk/*/synop/verSYNOP.*",intern=T)
#' cond     = list(obs="!is.na(obs)",
#'                 veri_description="grepl('forecast',veri_description)",
#'                 veri_forecast_time="veri_forecast_time%in%c(1200,16800)",
#'                 state="state%in%c(0,1)",
#'                 statid="!is.na(as.numeric(statid))")
#'
#' colnames = c("obs","veri_data","veri_forecast_time","veri_initial_date","varno","veri_model","statid")
#' DT       = fdbk_dt_multi_large(fnames,cond,colnames,cores=20)
#' keep     = comparableRows(DT,splitCol="veri_model",splitVal=c("GME       ","ICON      "),compareBy=c("veri_forecast_time","veri_initial_date","varno","statid"))
#' DT       = DT[keep]
#' gc()
#' 
#' scores                   = fdbk_dt_verif_continuous(DT,strat=c("veri_forecast_time","veri_initial_date","varno","veri_model"))
#' scores$veri_initial_date = as.POSIXct(scores$veri_initial_date,format="%Y%m%d%H")
#' scores$varno             = varno_to_name(scores$varno)
#' 
#' p = ggplot(scores[varno=="RH"&scorename=="RMSE",],aes(x=veri_initial_date,y=scores,color=factor(veri_forecast_time),linetype=veri_model,group=veri_model))+
#'     geom_line()+
#'     facet_grid(~scorename~varno~veri_forecast_time,scales="free")
#' p
#'
#' #EXAMPLE 6 (TEMP time series)
#' require(ggplot2)
#' require(RColorBrewer)
#' fnames      = system("/bin/ls ~/examplesRfdbk/*/temp/verTEMP.*",intern=T)
#' LEVELS      = c(100000,92500,85000,70000,50000,40000,30000,25000,20000,15000,10000,7000,5000,3000,2000,1000)
#' cond        = list(statid="!is.na(as.numeric(statid))",
#'                    obs="!is.na(obs)", 
#'                    state="state%in%c(0,1,5)",
#'                    veri_run_type="veri_run_type%in%c(0,4)", 
#'                    statid="round(as.numeric(statid)/1000)<=10",
#'                    level='level%in%c(100000,92500,85000,70000,50000,40000,30000,25000,20000,15000,10000,7000,5000,3000,2000,1000)',
#'                    veri_forecast_time="veri_forecast_time%in%c(0,4800,9600,14400,16800)")
#' columnnames = c("obs","veri_data","veri_forecast_time","veri_initial_date","level","varno","veri_model")
#' DT          = fdbk_dt_multi_large(fnames,cond,columnnames,cores=10)
#' DT[,valid_date:=as.POSIXct(veri_initial_date,format="%Y%m%d%H%M")+veri_forecast_time*36]
#' SCORES  = fdbk_dt_verif_continuous(DT,strat=c("veri_forecast_time","level","varno","valid_date","veri_model"))
#' SCORES[,varno:=varno_to_name(varno)]
#' x11(width=18,height=6)
#' ggplot(SCORES[scorename=="ME" & varno=="T"],aes(x=valid_date,y=as.numeric(factor(level)),fill=cut(scores,seq(-10,10,len=9))))+
#'          geom_raster(limits=c(-20,20))+
#'          facet_wrap(~veri_model~veri_forecast_time~varno,ncol=5)+
#'          scale_y_reverse(breaks = seq(length(LEVELS),1,by=-1),labels=rev(LEVELS))+
#'          scale_fill_manual("ME",values=rev(brewer.pal(9, "RdYlBu")),drop=F)+
#'          theme_bw()
#'
#' #EXAMPLE 7 (continuous scores by lead-time plus confidence intervals)
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/verSYNOP.*",intern=T)[1:10]
#' cond                  = list(varno="varno%in%c(3,4)",veri_description="grepl('forecast',veri_description)")
#' columnnames           = c("obs","veri_data","varno","veri_forecast_time")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' DT$varno              = varno_to_name(DT$varno)
#' strat                 = c("varno","veri_forecast_time")
#' scores                = fdbk_dt_verif_continuous(DT,strat,bootscores=T,R=100)
#' ggplot(scores, aes(x=veri_forecast_time, y=scores,color=varno)) + 
#'    geom_errorbar(aes(ymin=CI_L, ymax=CI_U), width=.1) +
#'    geom_line() +
#'    geom_point()+
#'    theme_bw()  +
#'    facet_wrap(~scorename,scale="free_y",ncol = 6)
fdbk_dt_verif_continuous <- function(DT,strat,bootscores=F,R=100){
  # scores using all data
  output = DT[,list(ME   = mean(veri_data-obs,na.rm=T),
                    SD   = sd(veri_data-obs,na.rm=T),
                    RMSE = sqrt(mean((veri_data-obs)^2,na.rm=T)),
                    MAE  = mean(abs(veri_data-obs),na.rm=T),
                    R2   = cor(obs,veri_data,use="pair")^2,
                    LEN  = as.numeric(.N)),by=strat]
  if (length(output)>0){
    output = melt(output,1:length(strat),(length(strat)+1):length(output),variable.name="scorename",value.name="scores",variable.factor=F,value.factor=F)
  }
  
  if (bootscores){
    # bootable score functions
    difference <- function(obs,d,veri_data){ return(veri_data[d]-obs[d]) }
    r2b       <- function(obs,d,veri_data){ return(cor(obs[d],veri_data[d],use="pair")^2) }
    
    # function returning a data.table with confidence intervals (5% & 95%)
    contscores_ci <- function(obs,veri_data,R){
      bt       = boot(obs,difference,R=R,veri_data=veri_data)$t
      CI_ME    = as.vector(quantile(rowMeans(bt,na.rm=T),c(.05,.95)))
      CI_MAE   = as.vector(quantile(rowMeans(abs(bt),na.rm=T),c(.05,.95)))
      CI_RMSE  = as.vector(quantile(sqrt(rowMeans(bt*bt,na.rm=T)),c(.05,.95)))
      CI_SD    = as.vector(quantile(rowSds(bt,na.rm=T),c(.05,.95)))
      CI_R2   = boot.ci(boot(obs,r2b, R=R,veri_data=veri_data), type="basic", conf = c(0.95))$basic[4:5]
      return(data.table(scorename=c("ME","MAE","RMSE","SD","R2"),
                        CI_L=c(CI_ME[1],CI_MAE[1],CI_RMSE[1],CI_SD[1],CI_R2[1]),
                        CI_U=c(CI_ME[2],CI_MAE[2],CI_RMSE[2],CI_SD[2],CI_R2[2])))
    }
    
    # calculate confidence intervals
    confintervals = DT[,contscores_ci(obs,veri_data,R=R),by=strat]
    
    # merge scores with confidence intervals
    output        = merge(output,confintervals,by=c(strat,"scorename"),all=TRUE)
  }
  
  return(output)
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Deterministic scores for wind direction in degrees with bootstrap confidence intervals if required
#'
#' @param DT data table (obs and veri_data are required,only for wind direction in degrees!)
#' @param strat list of variables to stratify for
#' @param bootscores logical if bootstrap confidence intervals are required (5-95)
#' @param R number of bootstrap iterations (default 100)
#'
#' @return a data.table of stratified continuous verification scores  (ME,SD,RMSE,R2,LEN)
#' @author Felix <felix.fundel@@dwd.de>
fdbk_dt_verif_continuous_windDir <- function(DT,strat,bootscores=F,R=100){
  output = DT[,list(ME=mean(windBias(veri_data,obs),na.rm=T),SD=sd(windBias(veri_data,obs),na.rm=T),RMSE=sqrt(mean((windBias(veri_data,obs))^2,na.rm=T)),MAE=mean(abs(windBias(veri_data,obs)),na.rm=T),R2=cor(obs,veri_data,use="pair")^2,LEN=as.numeric(.N)),by=strat]
  output = melt(output,1:length(strat),(length(strat)+1):length(output),variable.name="scorename",value.name="scores",variable.factor=F,value.factor=F)
  
  
  if (bootscores){
    difference <- function(obs,d,veri_data){ return(windBias(veri_data[d],obs[d])) }
    r2b       <- function(obs,d,veri_data){ return(cor(obs[d],veri_data[d],use="pair")^2) }
    
    # function returning a data.table with confidence intervals (5% & 95%)
    contscores_ci <- function(obs,veri_data,R){
      bt       = boot(obs,difference,R=R,veri_data=veri_data)$t
      CI_ME    = as.vector(quantile(rowMeans(bt,na.rm=T),c(.05,.95)))
      CI_MAE   = as.vector(quantile(rowMeans(abs(bt),na.rm=T),c(.05,.95)))
      CI_RMSE  = as.vector(quantile(sqrt(rowMeans(bt*bt,na.rm=T)),c(.05,.95)))
      CI_SD    = as.vector(quantile(rowSds(bt,na.rm=T),c(.05,.95)))
      CI_R2   = boot.ci(boot(obs,r2b, R=R,veri_data=veri_data), type="basic", conf = c(0.95))$basic[4:5]
      return(data.table(scorename=c("ME","MAE","RMSE","SD","R2"),
                        CI_L=c(CI_ME[1],CI_MAE[1],CI_RMSE[1],CI_SD[1],CI_R2[1]),
                        CI_U=c(CI_ME[2],CI_MAE[2],CI_RMSE[2],CI_SD[2],CI_R2[2])))
    }
    
    # calculate confidence intervals
    confintervals = DT[,contscores_ci(obs,veri_data,R=R),by=strat]
    
    # merge scores with confidence intervals
    output        = merge(output,confintervals,by=c(strat,"scorename"),all=TRUE)
  }
  
  return(output)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Calculates stratified contingency table entries (above threshold) for a data table
#'
#' 
#' @param DT data.table with relevant information
#' @param vars character vector of varnos (if NULL take from DT)
#' @param thrs list of vectors of thresholds for each varno (if NULL threshold are generated from quantiles)
#' @param by stratify contingency entries by these DT columns
#' @param cores number of CPU cores to split the calculation (helps for larger data tables)
#' @return data.table with columns varno,thr, hits,false,miss,corrneg and the arguments of 'by'
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE (CSI for quantile thresholds) 
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/verSYNOP.*",intern=T)
#' cond                  = list(veri_description="grepl('forecast',veri_description)",
#'                              veri_forecast_time="veri_forecast_time%in%c(2400,4800,7200,9600,12000,14400,16800)")
#' columnnames           = c("obs","veri_data","varno","veri_model","veri_forecast_time","statid","veri_initial_date")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' vars                  = c('1','3','4','29')
#' thrs                  = list('1'=c(50,60),'3'=c(-5,0,5),'4'=c(-5,0,5),'29'=c(.4,.6,.8))
#' xx                    = fdbk_dt_conttable(DT,vars=vars,thrs=thrs,by=c("veri_model","veri_forecast_time","varno"),cores=10)
#' CSI                   = xx[,list(csi =(hit)/(hit + miss + false) ),by=c("veri_forecast_time","veri_model","thr","varno")]
#' CSI[,varno:=varno_to_name(varno,T)]
#' ggplot(CSI,aes(x=thr,y=csi,color=factor(veri_forecast_time),linetype=factor(veri_model),group=interaction(varno,veri_forecast_time,veri_model)))+
#' geom_line()+
#' ggtitle("CSI")+
#' facet_wrap(~varno,scales="free_x")
fdbk_dt_conttable <- function(DT,vars=NULL,thrs=NULL,by=NULL,cores=1){
  if(!all(c("varno","obs","veri_data")%in%names(DT))){
    stop("DT needs at least columns 'varno','obs' and 'veri_data'")
  }
  if (is.null(vars)){ vars = as.character(unique(DT$varno))}
  if (is.null(thrs)){ thrs = lapply(vars,function(v){return(unique(quantile(DT[DT$varno==v]$obs,seq(.05,.95,by=.1))))}); names(thrs) = vars}
  inner <- function(var,thrs){
    inner2 <- function(t,v){
      return(DT[DT$varno==v,list(varno=v,thr=t,hit=sum(veri_data>=t & obs>=t), miss=sum(veri_data<t & obs>=t), false=sum(veri_data>=t & obs<t),corrneg=sum(veri_data<t & obs<t)),by=by])
    }
    return(do.call(.rbind.data.table,lapply(unlist(thrs[var]),inner2,var)))
  }
  return(do.call(.rbind.data.table,mclapply(vars,inner,thrs,mc.cores = cores)))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Calculates stratified contingency table entries (above or between thresholds) for a data table
#' 
#' @param DT data.table with relevant information (at least varno, obs and veri_data)
#' @param thrs list of variable having each a list of lower/upper thresholds (set upper to Inf if only one threshold is required)
#' @param by stratify contingency entries by these DT columns
#' @param cores computing cores for the outer loop (splits computation by varnos)
#' @param incores computing cores for the outer loop (splits computation by thresholds)(available cores have to be of number cores x incores)
#' @return data.table with columns varno,thr, hits,false,miss,corrneg and the arguments of 'by'
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE (CSI for quantile thresholds) 
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/verSYNOP.*",intern=T)
#' cond                  = list(veri_description="grepl('forecast',veri_description)",
#'                              veri_forecast_time="veri_forecast_time%in%c(2400,4800,7200,9600,12000,14400,16800)")
#' columnnames           = c("obs","veri_data","varno","veri_model","veri_forecast_time","statid","veri_initial_date")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' thrs                  = list('29'=list('lower'=c(.5,.8),'upper'=c(Inf,.9)),
#'                               '3'=list('lower'=c(-5,0,5),'upper'=c(Inf,Inf,Inf)))
#' xx                    = fdbk_dt_conttable_2thrs(DT,thrs,by=c("veri_model","veri_forecast_time","varno"),cores=2,incores=3)
#' CSI                   = xx[,list(csi =(hit)/(hit + miss + false) ),by=c("veri_forecast_time","veri_model","thr","varno")]
#' CSI[,varno:=varno_to_name(varno,T)]
#' ggplot(CSI,aes(x=veri_forecast_time,y=csi,group=interaction(veri_model,thr),linetype=veri_model,color=thr))+
#' geom_line()+
#' facet_grid(~varno)+
#' ggtitle("CSI")
fdbk_dt_conttable_2thrs <- function(DT,thrs,by,cores=1,incores=1){
  inner <- function(var,thrs,incores){
    inner2 <- function(tind,v,t){
      interv=c(t$lower[tind],t$upper[tind])
      return(DT[DT$varno==v,list(hit     = sum(veri_data%between%interv & obs%between%interv), 
                                 miss   = sum(!veri_data%between%interv & obs%between%interv), 
                                 false    = sum(veri_data%between%interv & !obs%between%interv),
                                 corrneg = sum(!veri_data%between%interv & !obs%between%interv),
                                 thr     = paste(interv[!is.infinite(interv)],collapse="-")),by=by])
    }
    return(do.call(.rbind.data.table,mclapply(seq(1,length(thrs[[var]]$lower)),inner2,var,thrs[[var]],mc.cores = incores)))
  }
  return(do.call(.rbind.data.table,mclapply(names(thrs),inner,thrs,incores,mc.cores = cores)))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Calculates most common contingeny scores
#' 
#' @param CONTTABLE data.table with colums hit,miss,corrneg,false and additional columns (output of fdbk_dt_conttable(_2thrs))
#' @param by stratify contingency entries by these columns
#' @param meltTable if TRUE (default) melt output so that there is one 'scores' column and one 'scorename' column
#' @return data.table with one column of score names and one column of scores values
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/verSYNOP.*",intern=T)
#' cond                  = list(veri_description="grepl('forecast',veri_description)",
#'                              veri_forecast_time="veri_forecast_time%in%c(2400,4800,7200,9600,12000,14400,16800)")
#' columnnames           = c("obs","veri_data","varno","veri_model","veri_forecast_time","statid","veri_initial_date")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' thrs                  = list('29'=list('lower'=c(.8,.6),'upper'=c(Inf,.9)),
#'                               '3'=list('lower'=c(-5,0,5),'upper'=c(Inf,Inf,Inf)))
#' CONTTABLE             = fdbk_dt_conttable_2thrs(DT,thrs,by=c("veri_model","veri_forecast_time","varno"),cores=2,incores=3)
#' SCORES                = fdbk_dt_contscores(CONTTABLE,by=c("veri_model","veri_forecast_time","varno","thr"))
#' ggplot(SCORES,aes(x=veri_forecast_time,y=scores,color=thr,linetype=veri_model))+
#' geom_line()+
#' geom_point()+
#' facet_grid(scorename~varno,scale="free_y")+
#' theme_bw()
fdbk_dt_contscores <- function(CONTTABLE,by,meltTable=T){
  OUT = CONTTABLE[,list(POD = (hit)/(hit+miss),
                              TSS  =  hit/(hit + miss)-false/(false+corrneg),
                              FAR  =  false/(hit + false),
                              FBI  = (hit+false)/(hit + miss),
                              OR   = (hit*corrneg)/(miss*false),
                              POFD = false/(corrneg+false),
                              SR   = hit/(hit+false), 
                              CSI  = hit/(hit+miss+false),
                              ACC  = (hit+corrneg)/(hit+miss+corrneg+false),
                              ORSS = (hit*corrneg - miss*false) / (hit*corrneg + miss*false),
                              ETS  = (hit-(hit+miss)*(hit+false)/(hit+miss+false+corrneg))/(hit+miss+false-(hit+miss)*(hit+false)/(hit+miss+false+corrneg)),
                              HSS  = ((hit+corrneg)-((hit+miss)*(hit+false)+(corrneg+miss)*(corrneg+false))/(hit+miss+false+corrneg))  /  ((hit+miss+false+corrneg)-((hit+miss)*(hit+false)+(corrneg+miss)*(corrneg+false))/(hit+miss+false+corrneg)),
                              NHIT = hit,
                              NMISS = miss,
                              NFALSE = false,
                              NCORRNEG = corrneg,
                              LEN  = hit+miss+corrneg+false),by]
  if(meltTable){
	OUT = melt(OUT,id=1:length(by), measure=(length(by)+1):(17+length(by)),variable.name = "scorename", value.name = "scores")
  }
  return(OUT)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Calculates stratified hit rates for uncertain obs/fcst
#' 
#' @param DT data.table with relevant information (at least varno, obs and veri_data)
#' @param thrs list of variable having each a list of lower/upper limit, relative to observation
#' @param by stratify contingency entries by these DT columns
#' @param cores computing cores for the outer loop (splits computation by varnos)
#' @param incores computing cores for the outer loop (splits computation by thresholds)(available cores have to be of number cores x incores)
#' @return data.table with columns varno,interval, hits, total and the arguments of 'by'
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE (CSI for quantile thresholds) 
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/verSYNOP.*",intern=T)
#' cond                  = list(veri_description="grepl('forecast',veri_description)",
#'                              veri_forecast_time="veri_forecast_time%in%c(2400,4800,7200,9600,12000,14400,16800)")
#' columnnames           = c("obs","veri_data","varno","veri_model","veri_forecast_time","statid","veri_initial_date")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' thrs                  = list('29'=list('lower'=c(-1/6),'upper'=c(1/6)),
#'                               '3'=list('lower'=c(-1,-2),'upper'=c(1,2)))
#' xx                    = fdbk_dt_hits_uncert(DT,thrs,by=c("veri_model","veri_forecast_time","varno"),cores=2,incores=3)
#' PEC                   = xx[,list(PEC =(hit)/(total) ),by=c("veri_forecast_time","veri_model","interval","varno")]
#' PEC[,varno:=varno_to_name(varno,T)]
#' ggplot(PEC,aes(x=veri_forecast_time,y=PEC,group=interaction(veri_model,interval),linetype=veri_model,color=interval))+
#' geom_line()+
#' geom_point()+
#' facet_grid(~varno)+
#' theme_bw()+
#' ggtitle("Percent Correct (within interval)")
fdbk_dt_hits_uncert <- function(DT,thrs,by,cores=1,incores=1){
  inner <- function(var,thrs,incores){
    inner2 <- function(tind,v,t){
      interv = c(t$lower[tind],t$upper[tind])
      return(DT[DT$varno==v,list(hit      = sum((veri_data-obs)%between%interv), 
                                 total    = .N,
                                 interval = paste(round(interv[!is.infinite(interv)],3),collapse="-")),by=by])
    }
    return(do.call(.rbind.data.table,mclapply(seq(1,length(thrs[[var]]$lower)),inner2,var,thrs[[var]],mc.cores = incores)))
  }
  return(do.call(.rbind.data.table,mclapply(names(thrs),inner,thrs,incores,mc.cores = cores)))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Calculate CRPS(crps, crpsPot,Reli) from data.table applied on selected parts of the table
#' (Caution, double check results! DT sorting might be modified!)
#' 
#' @param DT data.table (columns 'veri_ens_member','obs' and 'veri_data' plus all variables to make forecasts distinguishable are required!!!)
#' @param by stratify crps by (e.g. 'varno')
#' @return data.table with columns as defined in 'by' plus scorename plus score
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' #EXAMPLE 1 (CRPS for each varno)
#' fnames                = system("ls ~/examplesRfdbk/talagrand/*SYNOP*",intern=T)
#' cond                  = list(veri_description="grepl('first guess ensemble member',veri_description)",
#'                              obs="!is.na(obs)",
#'                              statid="!is.na(as.numeric(statid)) & !duplicated(statid)",
#'                              veri_forecast_time="veri_forecast_time==100",
#'                              state="state%in%c(0,1,5)")
#' columnnames           = c("veri_data","varno","obs","veri_ens_member","veri_initial_date","veri_forecast_time","statid")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,10)
#' DT[,varno:=varno_to_name(varno)]
#' fdbk_dt_crps(DT,by="varno")
#'
#' #EXAMPLE 2 (CRPS decomosition for forecasts at SYNOP stations)
#' require(ggplot2)
#' fnames    = system("/bin/ls ~/examplesRfdbk/eps/*12/verSYNOP*",intern=T)
#' condition = list(veri_description="grepl('member',veri_description)",
#'                  state="state%in%c(0,1)",
#'                  statid="round(as.numeric(statid)/1000)==10 & !duplicated(statid)",
#'                  veri_forecast_time="veri_forecast_time>=1200")
#' columns   = c("obs","veri_data","varno","veri_ens_member","veri_forecast_time","statid","veri_initial_date")
#' DT        = fdbk_dt_multi_large(fnames,condition,columns,5)
#' CRPS = fdbk_dt_crps(DT,by=c("varno","veri_forecast_time"))
#' CRPS[,varno:=varno_to_name(varno,F)]
#' ggplot(CRPS,aes(x=veri_forecast_time,y=score))+geom_line()+geom_point()+facet_grid(~varno~scorename,scales="free_y")+theme_bw()+ggtitle("SYNOP (DE) CRPS decomosition")
#' 
#' #EXAMPLE 3 (slow...)(CRPS decomosition for european forecasts at TEMP stations)
#' require(ggplot2)
#' fnames    = system("/bin/ls ~/examplesRfdbk/eps/*12/verTEMP*",intern=T)
#' condition = list(veri_description="grepl('member',veri_description)",
#'                  state="state%in%c(0,1)",
#'                  level="level%in%c(100000,92500,85000,75000,70000,50000,40000,30000,25000,20000,10000)",
#'                  statid="round(as.numeric(statid)/1000)<=10 & !duplicated(statid)",
#'                  veri_forecast_time="veri_forecast_time>=1200",
#'                  varno="varno!=1")
#' columns   = c("obs","veri_data","varno","level","veri_ens_member","veri_forecast_time","veri_initial_date","statid")
#' DT        = fdbk_dt_multi_large(fnames,condition,columns,5)
#' CRPS      = fdbk_dt_crps(DT,by=c("varno","level","veri_forecast_time"))
#' CRPS[,varno:=varno_to_name(varno,F)]
#' ggplot(CRPS,aes(x=score,y=level,color=factor(veri_forecast_time),group=veri_forecast_time))+
#'  geom_path()+facet_wrap(~varno~scorename,scale="free_x",ncol=3)+
#'  scale_y_reverse()+theme_bw()+scale_colour_discrete("lead-time")
fdbk_dt_crps <- function(DT,by){
  nmem       = length(unique(DT$veri_ens_member))
  uniq_olen  = unique(DT[,list(len=.N),by=c(names(DT)[!names(DT)%chin%c("obs","veri_data","veri_ens_member")])]$len)
  keycols    = paste(c(names(DT)[!names(DT)%chin%c("obs","veri_data","veri_ens_member")],"veri_ens_member"),collapse=",")
  eval(parse(text=paste("setkey(DT,",keycols,")",sep="")))
  if (!identical(as.integer(DT$veri_ens_member),rep(seq(1,nmem),dim(DT)[1]/nmem))){
    stop("unsufficient information in DT to uniquely eps forecasts to observations (typical fixes: add column or remove duplicated stations)")
  }
  DT[,c("crps","crpsPot","Reli"):=crpsDecomposition(obs=obs[seq(1,length(obs),by=nmem)],eps=t(matrix(veri_data,nrow=nmem)))[c(1,2,3)],by=by]
  eval(parse(text=paste("setkey(DT,",paste(by,collapse=","),")")))
  scores = DT[,c(by,"crps","crpsPot","Reli"),with=F]
  scores = scores[!duplicated(scores)]
  scores = melt(scores,1:length(by),(length(by)+1):dim(scores)[2],"scorename","score")
  DT[,c("crps","crpsPot","Reli"):=NULL,]
  return(scores)
}
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#' Calculate CRPS and Ignorance score from data.table with EPS mean/spread, assuming a normally distributed EPS
#' 
#' @param DT data.table (columns 'veri_description','obs' and 'veri_data' are required!!!) values of veri_description have to be "mean" or "spread"
#' @param by stratify crps by (e.g. 'varno')
#' @return data.table with columns as defined in 'by' plus scorename plus score
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' require(ggplot2)
#' fnames      = system("/bin/ls ~/examplesRfdbk/eps/*12/verTEMP*",intern=T)
#' condition   = list(
#'                            veri_description="grepl('first guess',veri_description)",
#'                            veri_description="grepl('ensemble',veri_description)",
#'                            state="state%in%c(0,1)",
#'                            level="level%in%c(100000,92500,85000,75000,70000,50000,40000,30000,25000,20000,10000)",
#'                            veri_forecast_time="veri_forecast_time>=1200",
#'                            varno="varno!=1")
#' vars        = c("obs","veri_data","varno","level","veri_description","veri_forecast_time")
#' DT          = fdbk_dt_multi_large(fnames,condition,vars,5)
#' DT[grepl("mean",veri_description),veri_description:="mean"]
#' DT[grepl("spread",veri_description),veri_description:="spread"]
#' by=c("varno","level","veri_forecast_time")
#' CRPS = fdbk_dt_crps_norm(DT,by)
#' CRPS[,varno:=varno_to_name(varno,F)]
#' CRPS[scorename=="IGN" & score>10000,score:=NA]
#' ggplot(CRPS,aes(x=score,y=level,color=factor(veri_forecast_time),group=veri_forecast_time))+
#'   geom_path()+geom_point()+facet_wrap(~scorename~varno,scale="free_x",ncol=4)+
#'   scale_y_reverse()+theme_bw()+scale_colour_discrete("lead-time")
fdbk_dt_crps_norm <- function(DT,by){
  DT[,c("CRPS","IGN"):=lapply(crps(obs=obs[veri_description=="mean"],pred=cbind(veri_data[veri_description=="mean"],veri_data[veri_description=="spread"]))[c(1,3)],mean,na.rm=T),by=by]
  eval(parse(text=paste("setkey(DT,",paste(by,collapse=","),")")))
  scores = DT[,c(by,"CRPS","IGN"),with=F]
  scores = scores[!duplicated(scores)]
  scores = melt(scores,1:length(by),(length(by)+1):dim(scores)[2],"scorename","score")
  DT[,c("CRPS","IGN"):=NULL,]
  return(scores)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#' Calculate the brier score (and decomposition and skill score) for one threshold per variable
#' 
#' @param DT data.table (columns 'veri_ens_member','obs' and 'veri_data' plus all variables to make forecasts distinguishable are required!!!)
#' @param thresholds list of threshold for variable names in DT (if "" uses obs median)
#' @param by stratify crps by (e.g. c('varno','veri_forecast_time'))
#' @return data.table with columns as defined in 'by' plus scorename plus score
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' require(ggplot2)
#' fnames    = system("/bin/ls ~/examplesRfdbk/eps/*12/verSYNOP*",intern=T)
#' condition = list(veri_description="grepl('member',veri_description)",
#'                  state="state%in%c(0,1)",
#'                  statid="!is.na(as.numeric(statid)) & !duplicated(statid)",
#'                  veri_forecast_time="veri_forecast_time>=1200")
#' columns   = c("obs","veri_data","varno","veri_ens_member","veri_forecast_time","statid","veri_initial_date")
#' DT        = fdbk_dt_multi_large(fnames,condition,columns,5)
#' PROBS     = fdbk_dt_brier(DT,by=c("varno","veri_forecast_time"))
#' 
#' ggplot(PROBS,aes(x=veri_forecast_time,y=score,color=scorename,group=scorename))+
#'   geom_line()+geom_point()+facet_wrap(~varno,scale="free_y",ncol=2)+
#'   theme_bw()+scale_colour_discrete("lead-time")
fdbk_dt_brier <- function(DT,thresholds="",by=""){
  nmem       = length(unique(DT$veri_ens_member))
  if (all(thresholds=="")){
    warning("Using median(s) as threshold(s)")
    thr=DT[,list(median=median(obs)),by=varno]
    thresholds = as.list(round(thr$median,3))
    names(thresholds)=thr$varno
  }
  if (all(by=="")){
    by = names(DT)[!names(DT)%chin%c("obs","veri_data","veri_ens_member")]
    warning (paste("Split scores for",paste(by,collapse=", ")))
  }
  
  keycols = paste(c(names(DT)[!names(DT)%chin%c("obs","veri_data","veri_ens_member")],"veri_ens_member"),collapse=",")
  eval(parse(text=paste("setkey(DT,",keycols,")",sep="")))
  if (!identical(as.integer(DT$veri_ens_member),rep(seq(1,nmem),dim(DT)[1]/nmem))){
    stop("unsufficient information in DT to uniquely eps forecasts to observations (typical fixes: add column or remove duplicated stations)")
  }
  
  DT[,FCID:=rep(seq(1,dim(DT)[1]/nmem),each=nmem)]
  PROBS = NULL
  for (i in 1:length(thresholds)){
    PROBS=.rbind.data.table(PROBS, DT[varno==as.integer(names(thresholds[i])),list(POB=sum(obs>thresholds[i])/nmem,PFC=sum(veri_data>thresholds[i])/nmem),by=c("FCID",by)])
  }
  DT[,FCID:=NULL]
  PROBS[,c("BS","BSS","REL","RES","UNC"):=brier(POB,PFC)[c(2,4,5,6,7)],by=by]
  PROBS[,c("POB","PFC","FCID"):=NULL]
  PROBS = PROBS[!duplicated(PROBS)]
  PROBS = melt(PROBS,1:length(by),(length(by)+1):dim(PROBS)[2],"scorename","score")
  for (i in 1:length(thresholds)){
    PROBS[varno==as.integer(names(thresholds[i])) ,varnon:=paste(varno_to_name(varno,F),"thr:",thresholds[i])]
  }
  PROBS[,varno:=varnon]
  PROBS[,varnon:=NULL]
  return(PROBS)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#' Calculate the reliability diagram statistics
#' 
#' @param DT data.table (columns 'veri_ens_member','obs' and 'veri_data' plus all variables to make forecasts distinguishable are required!!!)
#' @param thresholds list of threshold for variable names in DT (if "" uses obs median)
#' @param by stratify crps by (e.g. c('varno','veri_forecast_time'))
#' @param breaks breaks used to bin the forecast probabilities
#' @return data.table with columns forecast bin and observed frequency for each varno/threshold
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' require(ggplot2)
#' fnames    = system("/bin/ls ~/examplesRfdbk/eps/*12/verSYNOP*",intern=T)
#' condition = list(veri_description="grepl('member',veri_description)",
#'                  state="state%in%c(0,1)",
#'                  statid="!is.na(as.numeric(statid)) & !duplicated(statid)",
#'                  veri_forecast_time="veri_forecast_time>=1200")
#' columns   = c("obs","veri_data","varno","veri_ens_member","veri_forecast_time","statid","veri_initial_date")
#' DT        = fdbk_dt_multi_large(fnames,condition,columns,5)
#' ATTR      = fdbk_dt_reliability_diagram(DT,thresholds="",by=c("varno","veri_forecast_time"),breaks="")
#' ggplot(ATTR,aes(x=fbin,y=obin,color=factor(veri_forecast_time),group=veri_forecast_time))+geom_line()+geom_point()+facet_wrap(~varno)+ geom_abline(intercept = 0, slope = 1)+theme_bw()
fdbk_dt_reliability_diagram <- function(DT,thresholds="",by="",breaks=""){
  # number of members
  nmem = length(unique(DT$veri_ens_member))
  # if not threshold is selected use median
  if (all(thresholds == "")) {
    print("Using median(s) as threshold(s)")
    thr = DT[, list(median = median(obs)), by = varno]
    thresholds = as.list(round(thr$median, 3))
    names(thresholds) = thr$varno
  }
  # sort columns	
  keycols = paste(c(names(DT)[!names(DT) %chin% c("obs", "veri_data", "veri_ens_member")], "veri_ens_member"), collapse = ",")
  eval(parse(text = paste("setkey(DT,", keycols, ")", sep = "")))
  # check if all forecasts are distinguishable
  # and add a forecast id to DT
  if (!identical(as.integer(DT$veri_ens_member), rep(seq(1,nmem), dim(DT)[1]/nmem))) {
    stop("unsufficient information in DT to uniquely assign eps forecasts to observations (typical fixes: add column or remove duplicated stations)")
  }
  DT[, `:=`(FCID, rep(seq(1, dim(DT)[1]/nmem), each = nmem))]
  # calculate probability to exceed threshold for each variable
  PROBS = NULL
  for (i in 1:length(thresholds)) {
    PROBS = .rbind.data.table(PROBS, DT[varno == as.integer(names(thresholds[i])),list(POB = sum(obs > thresholds[i])/nmem, PFC = sum(veri_data > thresholds[i])/nmem),by = c("FCID", by)])
  }
  # delete forecast id from DT
  DT[, `:=`(FCID, NULL)]
  # create breaks for reliability diagram if not sepcified
  if (any(breaks=="")){
    breaks = seq(0,1,len=nmem+1)
  }
  labels = diff(breaks/2)+breaks[1:(length(breaks)-1)]
  # cut probabilities in bins	
  PROBS[,fbin:=cut(PROBS$PFC,breaks=breaks,labels=labels,include.lowest=T)]
  for (i in 1:length(thresholds)) {
    PROBS[varno == as.integer(names(thresholds[i])), `:=`(varnon,paste(varno_to_name(varno, F), "thr:", thresholds[i]))]
  }
  PROBS[,varno:=varnon]
  PROBS[,varnon:=NULL]
  # calculate and return occurence frequencies of bins
  ATTR = PROBS[,list(obin=sum(POB==1)/length(POB)),by=c("fbin",by)]
  #ATTR[,fbin:=as.numeric(as.character(ATTR$fbin))]
  ATTR[,fbin:=as.numeric(levels(ATTR$fbin))[ATTR$fbin]]
  return(ATTR)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################



#' Find comparable rows in DT for two or more attributes
#'
#' @param DT data.table
#' @param splitCol Dt column name that contains the attributes that should be compared
#' @param splitVal two or more values of splitCol that should be compared
#' @param compareBy other column names that should be used two decide if a comparable row exists for both splitVals 
#' @return indices of DT that show which rows should be retained (TRUE) i.e. rows that have a counterpart in each of the two splitVals
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#'
#' ## Delete rows in DT that have no counterpart for GME/ICON concerning the attributes: "veri_forecast_time","veri_initial_date","varno","statid"
#'
#' require(ggplot2)
#' fnames                = system("ls ~/examplesRfdbk/*/synop/verSYNOP.2014*",intern=T)
#' cond                  = list(varno="varno%in%c(3,29)",veri_description="grepl('forecast',veri_description)")
#' columnnames           = c("obs","veri_data","varno","veri_model","veri_forecast_time","veri_initial_date","obs_id")
#' DT                    = fdbk_dt_multi_large(fnames,cond,columnnames,20)
#' keepind               = comparableRows(DT,splitCol="veri_model",splitVal=unique(DT$veri_model),compareBy=c("obs","veri_forecast_time","veri_initial_date","varno","obs_id"))
#' DT                    = DT[keepind]
#' DT[,.N,by=c("varno","veri_model")]
#' DT$varno              = varno_to_name(DT$varno)
#' strat                 = c("varno","veri_forecast_time","veri_model")
#' scores                = fdbk_dt_verif_continuous(DT,strat)
#' p =  ggplot(scores,aes(x=veri_forecast_time,y=scores,group=interaction(scorename,varno,veri_model),colour = veri_model, linetype=varno))+
#'      geom_line(size=.7) + geom_point(size=1.5) + facet_wrap(~scorename, scales = "free")+
#'      theme_bw()+theme(axis.text.x  = element_text(angle=70,hjust = 1))
#' p
comparableRows <- function(DT,splitCol,splitVal,compareBy){
  eval(parse(text=paste0("DT[,XXLENDUMMYXX:=sum(",splitCol,"%in%splitVal)==length(splitVal),by=compareBy]")))
  keep = DT$XXLENDUMMYXX
  DT[,XXLENDUMMYXX:=NULL]
  return(keep)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################


#' Bin a data.table column around user defined levels and replace it with the levels value.
#' 
#' @description Other way to perform a binning like in function fdbk_dt_binning but by defining levels around which to bin 
#' instead of the bins limits. 
#' The limits of the bins will be calculated by taking the mean between neighbouring levels. The two functions differ in 
#' the sense that fdbk_dt_binning allow to have gaps between the bins, whereas the bins will be continuous in 
#' fdbk_dt_binning_level. This function allows to have non-equally spaced 
#' levels without gaps between the bins, so that the level is not always at the center of the bin.
#' @param  DT data.table
#' @param  varToBin variable that should be binned (and will be replaced by the binned version)
#' @param  levels number/vector of levels on which the bins will be defined
#' @param  Logical to include data that are out of the bins defined by levels. If set to FALSE (default), data that falls out 
#' of the bins are dicarded. If set to true, the numerically lower and upper limits will be set to -Inf and +Inf, respectively. 
#' This allows to keep data that falls out of the bins.
#' @return data.table with varToBin replaced by factorized mid-bin values  (NA if variable falls in none of the bins)
#
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @seealso \code{\link{cut}}
#'
#' @examples 
#' #plot scores accross binned levels
#' require(ggplot2)
#' fnames       = "~/examplesRfdbk/icon/temp/verTEMP.2014120112"
#' cond        = list(obs="!is.na(obs)",varno="varno%in%c(2,3,4,29)")
#' columnnames = c("obs","veri_data","varno","state","level")
#' DT          = fdbk_dt_multi_large(fnames,cond,columnnames,1)
#' levels = c(100000,92500,85000,70000,60000,50000,40000,30000)
#' DT = fdbk_dt_binning_level(DT,"level",levels)
#' DT$varno    = varno_to_name(DT$varno)
#' strat       = c("varno","level")
#' scores      = fdbk_dt_verif_continuous(DT,strat)
#' setkey(scores,scorename,varno,level)
#' scores      = scores[!is.na(scores),]
#' p =  ggplot(scores,aes(x=scores,y=level,group=interaction(varno,scorename)))+
#'  geom_path() + facet_wrap(~varno~scorename,scales="free_x",ncol = 6)+
#'  theme_bw()+theme(axis.text.x  = element_text(angle=70,hjust = 1))+scale_y_reverse()
#' p
fdbk_dt_binning_level <- function(DT,varToBin="level",levels,includeAll=FALSE){
  bins = getVarToBin(DT,varToBin) 
  levels = sort(levels,T) # Sort levels in decresaing order in case levels are not sorted 
  dp = diff(levels)/2
  binLower <- levels + dp[c(seq(along=dp), length(dp))]
  binUpper <- levels - dp[c(1, seq(along=dp))]
  
  if (includeAll){
    binLower[length(binLower)] = -Inf
    binUpper[1] = Inf
  }
  for (i in 1:length(binLower)){
    bins[DT[,varToBin,with=F]>=binLower[i] & DT[,varToBin,with=F]<binUpper[i] ] = levels[i]
  }
  
  DT[,varToBin] = bins
  
  return(DT)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Bin a data.table column into user defined bins and replace it with the bin center value.
#' If breaks can be provided (e.g. no gaps between bins) try to use 'cut' instead.
#' @param  DT data.table
#' @param  varToBin variable that should be binned (and will be replaced by the binned version)
#' @param  binLower number/vector lower bins limits
#' @param  binUpper number/vector upper bins limits
#' @return data.table with varToBin replaced by factorized mid-bin values  (NA if variable falls in none of the bins)
#
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @seealso \code{\link{cut}}
#'
#' @examples 
#' #plot scores accross binned levels
#' require(ggplot2)
#' fnames       = "~/examplesRfdbk/icon/temp/verTEMP.2014120112"
#' cond        = list(obs="!is.na(obs)",varno="varno%in%c(2,3,4,29)")
#' columnnames = c("obs","veri_data","varno","state","level")
#' DT          = fdbk_dt_multi_large(fnames,cond,columnnames,1)
#' binUpper    = seq(100000,1000,by=-5000)+1500
#' binLower    = seq(100000,1000,by=-5000)-1500
#' DT          = fdbk_dt_binning(DT,"level",binLower,binUpper)
#' DT          = DT[!is.na(level),,]
#' DT$varno    = varno_to_name(DT$varno)
#' strat       = c("varno","level")
#' scores      = fdbk_dt_verif_continuous(DT,strat)
#' setkey(scores,scorename,varno,level)
#' scores      = scores[!is.na(scores),]
#' p =  ggplot(scores,aes(x=scores,y=level,group=interaction(varno,scorename)))+
#'  geom_path() + facet_wrap(~varno~scorename,scales="free_x",ncol = 6)+
#'  theme_bw()+theme(axis.text.x  = element_text(angle=70,hjust = 1))+scale_y_reverse()
#' p
fdbk_dt_binning <- function(DT,varToBin="level",binLower,binUpper){
  bins = getVarToBin(DT,varToBin) 
  for (i in 1:length(binLower)){
    bins[DT[,varToBin,with=F]>=binLower[i] & DT[,varToBin,with=F]<binUpper[i] ] = (binLower[i]+binUpper[i])/2
  }
  DT[,varToBin] = bins
  return(DT)
}

###################
getVarToBin <- function(DT,varToBin) {
  bins = rep(NA,length(DT[,varToBin,with=F][[1]]))
  return(bins)
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#' Bin a data.table column into user defined bins and replace it with the bin center value.
#' If breaks can be provided (e.g. no gaps between bins) try to use 'cut' instead.
#' @param  DT data.table
#' @param  varToBin variable that should be binned (and will be replaced by the binned version)
#' @param  mode that will be used to defined the bin. Choices are "bin" or "level". In the first case the limtis of the bins have to be explicitly 
#' given in two vectors. The name given to the corresponding levels of the bin will be the mean of the lower and upper limit of the bin. In the second case a vector 
#' specifying the levels has to be given. The limits of the bins will be calculated by taking the mean between neighbouring levels. The two methods differ in the sense that 
#' the "bin" mode allow to have gaps between the bins, whereas the bins will be continuous in "level" mode. The "level" mode allow to have non-equally spaced 
#' levels without gaps between the bins, so that the level is not always at the center of the bin. 
#' @param  binLower number/vector lower bins limits
#' @param  binUpper number/vector upper bins limits
#' @param  levels number/vector of levels on which the bins will be defined
#' @return data.table with varToBin replaced by factorized mid-bin values  (NA if variable falls in none of the bins)
#
#' @author Josue <josue.gehring@@meteoswiss.ch>
#' 
#' @examples
#' # Example of linear interpolation based on an international standard atmosphere profile
#' require(ggplot2)
#' require(Rfdbk)
#' require(reshape2)
#' a1 = -6.5 # K/km standard atmosphere lapse rate, represents observations
#' a2 = -9 # K/km lapse rate obtained from a fictive model output
#' b1 = 288.15 # K standard atmosphere surface temperature
#' b2 = 295 # K surface temperature obtained from a fictive model output
#' Ho = 8.4 # km scale height
#' po = 1013.25 # standard atmosphere pressure in hPa
#' p = seq(250,1000,10) # pressure until the tropopause
#' T1 = a1*Ho*log(po/p)+b1 # Standard amtmosphere temperature profile
#' T2 =  a2*Ho*log(po/p)+b2 # Model output temperature profile 
#' Bias = T2-T1 # Bias = forecast - observation 
#' 
#' # Build a data table in feedback files format
#' obs = T1
#' veri_data = T2
#' veri_forecast_time = 24
#' veri_initial_date = 2015110900
#' time = -720
#' lat = 46.812
#' lon = 6.943
#' varno = 2
#' veri_model = "COSMO"
#' plevel = p
#' ident = 6610
#' levels = c(1000, 975, 950, 925, 900, 875, 850, 800, 750, 700, 650, 600, 550, 500, 450, 400, 350, 300, 250) # Levels on which to interpolate
#' DT = data.frame(obs,veri_data,veri_forecast_time,veri_initial_date,time,lat,lon,varno,veri_model,plevel,ident)
#' DT           = fdbk_dt_interpolate(DT,varToInter=c("obs","veri_data"), levelToInter = "plevel", interLevels = levels) # interpolate DT
#'
#' data1 = melt(data.frame(T1,p),id="T1") # Data for the standard atmosphere temperature profile
#' data2 = melt(data.frame(T2=DT$obs,DT$plevel),id="T2") # Interpolation of data1 
#' 
#' 
#' plot = ggplot() + geom_point(data=data1,aes(x=T1,y=value,colour=variable)) + geom_point(data=data2,aes(x=T2,y=value,colour=variable))+scale_y_reverse()+
#'   xlab("T [K]") + ylab("pressure [hPa]")+  scale_colour_manual(name="Temperature",values=c("red","blue"),labels=c("Interpolation","Standard atmosphere"))+theme(legend.position = "top")
#' print(plot) # plot of the Standard atmosphere profile and its interpolation 
#' 
#' allscores = fdbk_dt_verif_continuous(DT,strat=c("varno","veri_model","plevel") ) # Data table with scores
#' 
#' data3 = melt(data.frame(Bias,p),id="Bias") # Bias calculated directly from the standard atmosphere and model output profiles
#' ME = allscores[allscores$scorename=="ME"]$scores # scores calculated with fdbk_dt_verif_continuous on interpolation levels
#' ME_levels = allscores[allscores$scorename=="ME"]$plevel # interpolation levels
#' data4 = melt(data.frame(ME,ME_levels),id="ME")
#' plot2 = ggplot() + geom_point(data=data3,aes(x=Bias,y=value,colour=variable)) + geom_point(data=data4,aes(x=ME,y=value,colour=variable))+scale_y_reverse()+
#'   xlab("T bias [K]") + ylab("pressure [hPa]")+scale_colour_manual(name="Bias",values=c("red","blue"),labels=c("Interpolation","Standard atmosphere"))+theme(legend.position = "top")
#' print(plot2) # plot of the bias calculated directly from the profiles and the bias from the interpolation

fdbk_dt_interpolate <- function(DT,varToInter=c("obs","veri_data"), levelToInter = "plevel", interLevels = levels, varno="varno"){
  

  Ncol = ncol(DT)
  n = nrow(DT)
  OneSounding = as.data.frame(matrix(,nrow=1,Ncol)) # vector to store one single sounding at a time and interpolate it
  Colnames = colnames(DT)
  colnames(OneSounding) = Colnames
  OneSounding[1,] = DT[1,] # Store the first row of DT in the first row of OneSounding
  newDT = c()
  k = 1
  for (i in 2:n){
    
    soundingDone = TRUE # Boolean if i correspond to the end of a souding 
    checknames <- c("ident", "veri_forecast_time", "veri_initial_date", "time")
    if (all(DT[i,checknames] == DT[i-1,checknames])){
      soundingDone = FALSE # if previous row in DT is the same sounding as the current row, sounding is not done
      k = k+1
      OneSounding[k,] = DT[i,] # Store ther current row of DT in the current sounding
    } 
    if (soundingDone==TRUE | i==n){ # if the end of a sounding or the end of DT is reached, interpolate the current sounding and store it DTT
      OneSounding = na.omit(OneSounding)
      ik = unique(OneSounding[[varno]]) # Vector of all the different variables measured in one sounding 
      ds3 = as.numeric(OneSounding[[varno]])
      for (k in ik){
        kk = ds3 == k 
        OneVarno = OneSounding[kk,] # Vector of one souding for just one variable
        x = log(OneVarno[[levelToInter]])  # Take the variable levelToInter
        
        # interX is the range of interLevels that is contained in the sounding
        interX = interLevels[log(interLevels)<=max(x) & log(interLevels)>=min(x)]
        if (length(x) > 2 & length(interX)>2) {
          inter = matrix(,nrow=length(interX),ncol=length(varToInter))          
          for (l in 1:length(varToInter)){ # Interpolate for all variables
            
            y = OneVarno[[varToInter[l]]] # Take the values to interpolate
            inter[,l] = approx(x,y,log(interX))$y # Do the Interpolation
          }
          
          
          inter = as.data.frame(inter)
          
          colnames(inter) = varToInter # Give the colunames to inter
          DTT = data.frame(inter,OneVarno[,3:Ncol][1:length(interX),1:(Ncol-2)])
          DTT[[levelToInter]] = interX
          newDT = .rbind.data.table(newDT,DTT) # Bind DTT and newDT
        }  
      }
      OneSounding = as.data.frame(matrix(,nrow=200,Ncol)) # re-allocate OneSounding for the next sounding 
      colnames(OneSounding) = Colnames
      OneSounding[1,] = DT[i,] 
      k = 1 # Restart the level counter for the next sounding 
    }
  }
  return(newDT)
} 

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#' Calculate wind speed from u and v wind components in a data.table
#'
#' @param DATATABLE data table containing the columns "varno" with elements 3 and 4, and e.g. "obs", "obs_ini", "veri_data" or combinations of it
#' @param fcst forecast vector
#'
#' @return data.table with same columns as DATATABLE and varno=112
#'
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @examples 
#' fnames   = system("ls ~/examplesRfdbk/icon/synop/*",intern=T)[1:5]
#' cond     = list(obs                = "!is.na(obs)",
#'                 veri_run_class     = "veri_run_class%in%c(0,2)",
#'                 veri_run_type      = "veri_run_type%in%c(0,4)",
#'                 state              = "state%in%c(0,1,5)",
#'                 statid             = "!is.na(as.numeric(statid))",
#'                 statid             = "!duplicated(statid)",
#'                 varno              = "varno%in%c(3,4)")
#' colnames  = c("obs","veri_data","veri_forecast_time","veri_initial_date","lat","lon","varno","veri_model","statid","z_station")
#' DT        = fdbk_dt_multi_large(fnames,cond,colnames,cores=5)
#' SPD       = fdbk_dt_uv2spd(DT)  
#' .rbind.data.table(DT,SPD)
fdbk_dt_uv2spd <- function(DATATABLE,col=c("obs","veri_data")){
  by_distinct = names(DATATABLE)[!names(DATATABLE)%in%c(col,"varno")]
  setkey(DATATABLE,varno)
  return(eval(parse(text=paste0("DATATABLE[varno%in%c(3,4),list(",paste0(paste0(col,"=windSpeed(",col,"[1],",col,"[2])"),collapse=","),",varno=112),by=by_distinct]"))))
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#' Calculate wind direction from u and v wind components in a data.table
#'
#' @param DATATABLE data table containing the columns "varno" with elements 3 and 4, and e.g. "obs", "obs_ini", "veri_data" or combinations of it
#' @param fcst forecast vector
#'
#' @return data.table with same columns as DATATABLE and varno=111
#'
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @examples
#' fnames   = system("ls ~/examplesRfdbk/icon/synop/*",intern=T)[1:5]
#' cond     = list(obs                = "!is.na(obs)",
#'                 veri_run_class     = "veri_run_class%in%c(0,2)",
#'                 veri_run_type      = "veri_run_type%in%c(0,4)",
#'                 state              = "state%in%c(0,1,5)",
#'                 statid             = "!is.na(as.numeric(statid))",
#'                 statid             = "!duplicated(statid)",
#'                 varno              = "varno%in%c(3,4)")
#' colnames  = c("obs","veri_data","veri_forecast_time","veri_initial_date","lat","lon","varno","veri_model","statid","z_station")
#' DT        = fdbk_dt_multi_large(fnames,cond,colnames,cores=5)
#' DRC       = fdbk_dt_uv2drc(DT)  
#' .rbind.data.table(DT,DRC)
fdbk_dt_uv2drc <- function(DATATABLE,col=c("obs","veri_data")){
  by_distinct = names(DATATABLE)[!names(DATATABLE)%in%c(col,"varno")]
  setkey(DATATABLE,varno)
  return(eval(parse(text=paste0("DATATABLE[varno%in%c(3,4),list(",paste0(paste0(col,"=windDir(",col,"[1],",col,"[2])"),collapse=","),",varno=111),by=by_distinct]"))))
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Fast version of the 2AFC for continuous observations and forecasts
#' The score is based on the rank correlation coefficient
#'
#' @param obsv observation vector
#' @param fcst forecast vector
#'
#' @return afc score
#'
#' @author Felix <felix.fundel@@dwd.de>
afc <- function(obsv,fcst){
  ind = !(is.na(obsv) | is.na(fcst) | is.infinite(obsv) | is.infinite(fcst))
  if (all(!ind)){
    return(NaN)
  }else{
    return((1 + cor.fk(obsv[ind],fcst[ind]))/2)
  }
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Standard deviation on rows of array (faster than using 'apply')
#'
#' @param a 2d array
#'
#' @return standard deviation on rows
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' 
#' a = array(rnorm(1e5),dim=c(1000,50))
#' system.time(rowSds(a))
#' system.time(apply(a,1,sd))
#' # Results agree besides some numerical precision errors
#' identical(round(rowSds(a),12),round(apply(a,1,sd),12))
rowSds <- function(a,na.rm=F){
  return(sqrt(rowSums((a-rowMeans(a,na.rm=na.rm))**2,na.rm=na.rm)/(rowSums(a/a,na.rm=na.rm)-1)))
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Convert variable number (varno) to long or short variable name and reverse
#' @param varno(s) or short name(s)
#' @param short short or long name (boolean)
#' @param rev TRUE: from varno to name, FALSE: from short name to varno
#' @return long or short variable name(s)
#'
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @examples
#'
#' varno_to_name(c(3,4),short=T,rev=F)
#' varno_to_name(c(3,4),short=F,rev=F)
#' varno_to_name(c("RH","TS"),short=T,rev=T)
#' varno_to_name(c("RH","TS"),short=F,rev=T)
#' varno_to_name("geopotential (m^2/s^2)",short=F,rev=T)
#' varno_to_name(varno_to_name("geopotential (m^2/s^2)",short=F,rev=T))
varno_to_name <- function(varno,short=T,rev=F){
    data(VN)
    VN$longName = gsub("^\\s+|\\s+$", "", gsub("([[:space:]])|\\s+"," ", VN$longName))
    VN$shortName = gsub("^\\s+|\\s+$", "", VN$shortName)
    if (short & !rev)  return(str_trim(VN$shortName[match(varno, VN$varno)]))
    if (!short & !rev) return(str_trim(VN$longName[match(varno, VN$varno)]))
    if (short & rev)   return(VN$varno[match(varno, str_trim(VN$shortName))])
    if (!short & rev)  return(VN$varno[match(varno, str_trim(VN$longName))])
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Multiple plot function
#'
#' description ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
#' If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
#' then plot 1 will go in the upper left, 2 will go in the upper right, and
#' 3 will go all the way across the bottom.
#' @references  http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_\%28ggplot2\%29/
#'
#' @param cols:   Number of columns in layout
#' @param layout: A matrix specifying the layout. If present, 'cols' is ignored.
multiplot <- function(..., plotlist=NULL, cols=1, layout=NULL) {
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Convert u,v wind in wind direction in degrees
#'
#' @param u u wind vector
#' @param v v wind vector
#'
#' @return wind direction in degree (0 - <360), 360 is set to 0, if u&v=0 then return NA
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' u = c( 10,   0,   0, -10,  10,  10, -10, -10, 0)
#' v = c(  0,  10, -10,   0,  10, -10,  10, -10, 0)
#' windDir(u,v)
windDir <- function(u, v){
  radi                               = 1/0.0174532925199433
  direction                          = atan2(u,v)*radi + 180
  direction[round(direction,8)==360] = 0
  direction[u==0 & v==0]             = NA
  return(direction)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Convert u,v wind in wind speed
#'
#' @param u u wind vector
#' @param v v wind vector
#'
#' @return wind speed
#'
#' @author Felix <felix.fundel@@dwd.de>
windSpeed <- function(u, v){
  return(sqrt(u^2+v^2))
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Difference in wind direction (based un U. Pflügers code)
#'
#' @param ang_obs observed wind direction
#' @param ang_pred forecast wind direction
#'
#' @return wind direction difference in degree
#'
#' @author Felix <felix.fundel@@dwd.de>
windBias <- function(ang_pred,ang_obs){
  wdiff      = ang_pred-ang_obs
  ind        = abs(wdiff)>180
  ind        = ind[!is.na(ind)]
  wdiff[ind] = wdiff[ind]-sign(wdiff[ind])*360
  return(wdiff)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Scatterplot with colored points
#'
#' @param x numeric vector
#' @param y numeric vector
#' @param z numeric vector
#' @param zlim plot color range (default z range)
#' @param ncol number of colors (default 10)
#' @param cpal color palette (default red,white,blue)
#'
#' @return a plot
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' condition  = list(obs="!is.na(obs)",level="level%in%c(921)",statid="statid=='METOP-1   '",veri_forecast_time="veri_forecast_time==0",veri_run_type="veri_run_type==3",veri_ens_member="veri_ens_member==-1")
#' DT         = fdbk_dt_multi_large("~/examplesRfdbk/example_monRad/monRAD_2014092406.nc",condition,vars=c("obs","lon","lat"),cores=1)
#' x11(width=12,height=7.5)
#' DT[,scatterplot(lon,lat,obs,pch=20,cpal=tim.colors(),ncol=20,cex=.5)]
#' world(add=T,col="gray",fill=T)
scatterplot <- function (x, y, z, zlim = NULL, ncol = 10, cpal = c("red", "white", "blue"), ...){
  col.p <- colorRampPalette(cpal)
  if (is.null(zlim)) {
    breaks <- seq(min(z, na.rm = T) - 1e-12, max(z, na.rm = T) + 
                    1e-12, len = ncol)
  }
  else {
    breaks <- seq(zlim[1], zlim[2], len = ncol)
  }
  sc.col <- col.p(ncol)[as.numeric(cut(z, breaks = breaks))]
  par(mar = c(4, 4, 3, 8))
  plot(x, y, col = sc.col, ...)
  image.plot(legend.only = TRUE, zlim = range(breaks), col = col.p(ncol))
}



########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Convert WMO station-id to region
#'
#' @param ident numeric vector of station ID as integer (see variable "ident" in feedback file)
#' @return vector of same length wiith id replaced by region shortcut
#'
#' @author Felix <felix.fundel@@dwd.de>
statid_to_wmoregion<-function(ident){
  output = rep("ALL",length(ident))
  # AFRICA
  output[ident%between%c(60000,69999)] = "AF"
  #ASIA
  output[ident%between%c(20000,20099) | ident%between%c(20200,21999)|ident%between%c(23000,25999)  | ident%between%c(28000,32999) | ident%between%c(35000,36999) | ident%between%c(38000,39999) | ident%between%c(40350,48599) | ident%between%c(48800,49999) | ident%between%c(50000,59999)] = "AS"
  # SOUTH AMERICA
  output[ident%between%c(80000,88999)] = "SA"
  #NORTH AND CENTRAL AMERICA
  output[ident%between%c(70000,79999)] = "NA"
  # SOUTH-WEST PACIFIC
  output[ident%between%c(48600,48799) | ident%between%c(90000,98999)] = "SP"
  # EUROPE
  output[ident%between%c(1,19999) | ident%between%c(20100,20199) | ident%between%c(22000,22999) | ident%between%c(26000,27999) | ident%between%c(33000,34999) | ident%between%c(37000,37999) | ident%between%c(40000,40349) ] = "EU"
  # ANTARTICA
  output[ident%between%c(89000,89999)] = "AA"
  return(output)
}




########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Aggregate deterministic scores
#'
#' @param SCORENAME score name string
#' @param RMSE rmse scores of data subsets
#' @param ME me scores of data subsets
#' @param MSE mse scores of data subsets
#' @param SD sd scores of data subsets
#' @param MAE mae scores of data subsets
#' @param LEN length of forecast-observation pairs in subsets
#' @return pooled score value
#'
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @examples
#' x    = runif(1000)fnames   = system("ls ~/examplesRfdbk/icon/synop/*",intern=T)
#' y    = rnorm(1000)
#' x1   = x[1:10];x2=x[11:300];x3=x[301:1000]
#' y1   = y[1:10];y2=y[11:300];y3=y[301:1000]
#' rmse = function(x,y){return(sqrt(mean((x-y)^2)))}
#' rmse(x,y)
#' agg_det_scores("RMSE",RMSE=c(rmse(x1,y1),rmse(x2,y2),rmse(x3,y3)),LEN=c(length(x1),length(x2),length(x3)))
agg_det_scores <- function(SCORENAME=NULL,RMSE=NULL,ME=NULL,MSE=NULL,SD=NULL,MAE=NULL,LEN=NULL){
  # POOL MEs TO GLOBAL ME
  if(SCORENAME=="ME" & !is.null(ME) & !is.null(LEN)){  SCORE_agg = sum(LEN*ME,na.rm=T)/sum(LEN,na.rm=T)}
  # POOL MEs TO GLOBAL ME
  else if(SCORENAME=="MAE" & !is.null(MAE) & !is.null(LEN)){  SCORE_agg = sum(LEN*MAE,na.rm=T)/sum(LEN,na.rm=T)}
  # POOL MEs TO GLOBAL ME
  else if(SCORENAME=="MSE" & !is.null(MSE) & !is.null(LEN)){  SCORE_agg = sum(LEN*MSE,na.rm=T)/sum(LEN,na.rm=T)}
  # POOL RMSEs TO GLOBAL RMSE
  else if(SCORENAME=="RMSE"  & !is.null(RMSE) & !is.null(LEN)){ SCORE_agg = sqrt(sum(LEN*RMSE^2,na.rm=T)/sum(LEN,na.rm=T))}
  # POOL SD OF ERROR TO GLOBAL SD OF ERROR
  else if (SCORENAME=="SD" & !is.null(MSE) & !is.null(ME) & !is.null(LEN)){  SCORE_agg =  sqrt(sum(LEN*MSE,na.rm=T)/sum(LEN,na.rm=T) - (sum(LEN*ME,na.rm=T)/sum(LEN,na.rm=T))^2)}
  else{warning("MISSING RIGHT INPUT");return()}
  return(SCORE_agg)
  
}


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Get reference date(s) from feedback file(s)
#'
#' @param filenames filename(s) fo feedback file(s) inluding path
#' @return vector of reference dates YYYYmmddHHMM
#'
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @examples
#' filenames  = system("ls ~/examplesRfdbk/icon/synop/*",intern=T)
#' fdbk_refdate(filenames)
fdbk_refdate <- function(filenames){
  ref_date = rep(NA,length(filenames))
  for (i in 1:length(filenames)){
    f           = open.nc(filenames[i])
    ref_date[i] = att.get.nc(f, "NC_GLOBAL", "verification_ref_date")*10000+att.get.nc(f, "NC_GLOBAL", "verification_ref_time")
    close.nc(f)
  }
  return(ref_date)
}



########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Update a feedback file data.table with observations valid at initialization 
#' (helpful for calculation of tendency correlations or persistence scores)
#'
#' @param DT data.table with feedback file content, minimum requires "obs","level","varno","lon","lat" and "veri_initial_date" as YYYYmmddHHMM numeric and a column called "lonlat":=paste0(lon,lat)
#' @param fnamepast vector of filenames (including path) of feedback files that should be valid at times needed to fill DT (e.g. files of past 7 days to fill DT for a model of 7 day forecast range) 
#' @param cond list of conditions used for loading DT
#' @return DT with an additional columns "obs_ini"
#'
#' @author Felix <felix.fundel@@dwd.de>
#'
#' @examples
#' fdbkDir   = "~/examplesRfdbk/icon/synop"
#' fileName  = tail(dir(fdbkDir,full.names=T),1)
#' vars      = c("obs","veri_data","veri_forecast_time","level","varno","lon","lat","veri_initial_date")
#' cond      = ""
#' DT        = fdbk_dt_multi_large(fileName, condition=cond, vars=vars, cores=1)
#' DT[,lonlat:=paste0(lon,lat)]
#' fileNames = tail(dir(fdbkDir,full.names=T),10)
#' DT = fdbk_dt_add_obs_ini(DT,fileNames,cond)
#' DT[,lonlat:=NULL]
#' Plot correlation between observations for different lead-times
#' na.omit(DT[,list(cor=cor(obs,obs_ini,use="pairwise.complete.obs")),by=c("veri_forecast_time","varno")])[,ggplot(.SD,aes(x=veri_forecast_time,y=cor))+geom_line()+facet_wrap(~varno,scales="free")]
fdbk_dt_add_obs_ini<-function (DT, fnamepast, cond = cond) 
{
    refDates = fdbk_refdate(fnamepast)
    fillFiles = fnamepast[refDates %in% unique(DT$veri_initial_date)]
    if (length(fillFiles) > 0) {
        iniDates = refDates[refDates %in% unique(DT$veri_initial_date)]
        XX = c()
        vars = names(DT)[names(DT)%in%c("obs", "varno", "lat", "lon","level")]
        for (i in 1:length(fillFiles)) {
            DTFILL = unique(fdbk_dt_multi_large(fillFiles[i], 
                condition = cond, vars = vars, cores = 1))
            setnames(DTFILL, "obs", "obs_ini")
            DTFILL[, `:=`(lonlat, paste0(lon, lat))]
            DTFILL[, `:=`(lon, NULL)]
            DTFILL[, `:=`(lat, NULL)]
            DTFILL[, `:=`(veri_initial_date, as.character(iniDates[i]))]
            XX = .rbind.data.table(XX, DTFILL)
            rm(DTFILL)
        }
        keep = which(!duplicated(XX[, -c("obs_ini"), with = F]))
        XX = XX[keep]
	if ("level"%in%vars){
	        DT = merge(DT, XX, by = c("veri_initial_date", "varno", "level","lonlat"), all.x = T)
	}else{
		DT = merge(DT, XX, by = c("veri_initial_date", "varno", "lonlat"), all.x = T)
	}
    }
    else {
        DT[, `:=`(obs_ini, NA)]
    }
    return(DT)
}

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#' Non-overlapping regions, specifically defined for the DWD SYNOP verification
#'
#' @param lon longitude vector
#' @param lat latitude vector
#'
#' @return a vector of same lenght as lon or lat with character strings of the region for each point, NA for no match
#'
#' @author Felix <felix.fundel@@dwd.de>
#' @examples
#' DT = data.table(lon=c(15,85),lat=c(-30,40))
#' DT[,region:=lonlat_to_synopregion(lon,lat)]
#' DT
lonlat_to_synopregion <- function(lon,lat){
	output = rep(NA, length(lon))
	#output[lon%between%c(-2.999999,20) & lat%between%c(46.000001,57)] 	= "Central Europe"
	output[lon%between%c(60.000001,90) & lat%between%c(50.000001,60)] 	= "S-W Siberia"
	output[lon%between%c(60.000001,90) & lat%between%c(60.000001,70)] 	= "N-W Siberia"
	output[lon%between%c(90.000001,130) & lat%between%c(50.000001,70)] 	= "E Siberia"
	output[lon%between%c(-9.999999,40) & lat%between%c(0.000001,30)] 	= "N Africa"
	output[lon%between%c(10.000001,40) & lat%between%c(-34.999999,0)] 	= "S Africa"
  	output[lon%between%c(-119.999999,-100) & lat%between%c(30.000001,50)]	= "W Northamerica"
	output[lon%between%c(-99.999999,-70) & lat%between%c(30.000001,50)] 	= "E Northamerica"
	output[lon%between%c(-119.999999,-70) & lat%between%c(50.000001,70)] 	= "N Northamerica"
	output[lon%between%c(-79.999999,-40) & lat%between%c(-29.999999,0)] 	= "N Southamerica"
	output[lon%between%c(-79.999999,-40) & lat%between%c(-49.999999,-30)] 	= "S Southamerica"
	output[lon%between%c(80.000001,130) & lat%between%c(20.000001,50)] 	= "China"
	output[lon%between%c(102.5000001,175) & lat%between%c(-44.999999,17.5)]	= "AUS,NZ,INDON"
	output[lon%between%c(-180,180) & lat%between%c(-90,-60)] 		= "Antarctic"
	output[lon%between%c(55.000001,80) & lat%between%c(35.000001,50)] 	= "Kazakhstan"
	output[lon%between%c(-180,180) & lat%between%c(70.000001,90)] 		= "Arctic"
	return(output)
}















