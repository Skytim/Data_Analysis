library(data.table)
library(reshape2)
library(magrittr)
library(plyr)
library(dplyr)
library(tidyr)
library(purrr)
library(pipeR)
library(randomForest)
library(stringr)   
library(microbenchmark)
library(ALS)
dat_view <- fread('view_step1.csv',sep=',') %>>% tbl_dt(F) %>>%
  transform(dates=as.Date(dates),
            storeid=as.character(storeid)) %>>% tbl_dt(F)
dat_order <- readLines(file('order_step1.csv', encoding = "BIG5")) %>>% 
  strsplit(split=',') %>>% (do.call(rbind,.)) %>>% 
  (~a=.[1,]) %>>% 
  tbl_dt(F) %>>% setnames(names(.),a) %>>% (.[-1,]) %>>%
  transform(amt=as.numeric(amt))  %>>% 
  (~y = substr(.$datestime,start=1,stop=4)) %>>%
  (~m = substr(.$datestime,start=5,stop=6)) %>>% 
  (~d = substr(.$datestime,start=7,stop=8)) %>>%
  mutate(dates = as.Date(paste(paste(y,m,sep='-'),d,sep='-'))) %>>% tbl_dt(F)
dat_order_testing <- dat_order %>>% filter(month(dates)==9)  
dat_order_training <- dat_order %>>% filter(month(dates)!=9) %>>% 
  mutate(Mondiff=unique(month(dat_order_testing$dates))-month(dates)) %>>% 
  (~order_a = df ~ df  %>>% group_by(userid,storeid,Mondiff) %>>% 
     summarise(Number=n()) %>>% ungroup() %>>%
     mutate(Type=paste('Storeid_',storeid,'.','Mondiff_',Mondiff,'.','order',sep='')) %>>%
     dplyr::select(userid,Type,Number) %>>% spread(Type,Number,fill=0) %>>% 
     group_by(userid) %>>% summarise_each(funs(sum)))
dat_view_training <- dat_view %>>% filter(month(dates)!=9) %>>% 
  mutate(Mondiff=unique(month(dat_order_testing$dates))-month(dates)) %>>% 
  (~view_a = df ~ df %>>% group_by(userid,storeid,Mondiff) %>>% 
     summarise(Number=n()) %>>% ungroup() %>>%
     mutate(Type=paste('Storeid_',storeid,'.','Mondiff_',Mondiff,'.','view',sep='')) %>>%
     dplyr::select(userid,Type,Number) %>>% spread(Type,Number,fill=0) %>>% 
     group_by(userid) %>>% summarise_each(funs(sum)))
order_target <- dat_order %>>% filter(month(dates)!=6) %>>% 
  mutate(Mondiff=max(unique(month(dates)))+1-month(dates)) %>>% group_by(userid,storeid,Mondiff) %>>%
  summarise(Number=n()) %>>% mutate(Type=paste('Storeid_',storeid,'.','Mondiff_',Mondiff,'.','order',sep='')) %>>%
  dplyr::select(userid,Type,Number) %>>% spread(Type,Number,fill=0) %>>%
  group_by(userid) %>>% summarise_each(funs(sum))
view_target <- dat_view %>>% filter(month(dates)!=6) %>>%
  mutate(Mondiff=max(unique(month(dates)))+1-month(dates)) %>>% group_by(userid,storeid,Mondiff) %>>%
  summarise(Number=n()) %>>% mutate(Type=paste('Storeid_',storeid,'.','Mondiff_',Mondiff,'.','view',sep='')) %>>%
  dplyr::select(userid,Type,Number) %>>% spread(Type,Number,fill=0) %>>%
  group_by(userid) %>>% summarise_each(funs(sum))
dat_target <- merge(view_target,order_target,by="userid",all.x=TRUE,all.y=TRUE) %>>% tbl_dt(F) %>>% 
  lapply(function(x){x[is.na(x)]=0
  return(x)}) %>>% as.data.frame %>>% transform(userid=as.character(userid))
store <- base::sort(unique(dat_order$storeid) %>>% as.numeric) %>>% as.character
summary_result <- store %>>% alply(1,function(i){
  dd <- dat_order_testing %>>% filter(storeid==i)  %>>% group_by(userid) %>>% summarise(Response=1) %>>% ungroup() 
  dat_analysis <- merge(view_a,order_a, by = "userid", all.x = TRUE, all.y = TRUE) %>>% tbl_dt(F) %>>% 
    merge(dd, by = "userid", all.x = TRUE, all.y = FALSE) %>>% tbl_dt(F) %>% dplyr::select(-userid) %>>% 
    lapply(function(x){x[is.na(x)]=0 
    return(x)}) %>>% as.data.frame %>>% transform(Response=as.factor(Response)) %>>% 
    (~dat_analysis_no = df ~ df %>>%filter(Response=='0') %>>% unique) %>>%
    (~dat_analysis_yes = df ~ df %>>%filter(Response=='1'))
  dat_analysis1 <- rbind(dat_analysis_no,dat_analysis_yes)
  no_index <- setdiff((1:length(dat_analysis1$Response)),which((dat_analysis1$Response)=='1'))
  aa <- table(dat_analysis1$Response) %>>% as.numeric
  Model_prob.target <- rep(0,length(dat_target$userid))
  Model_prob.efficiency <- rep(0,length(dat_analysis$Response))
  Number_repeat <- ifelse(i=='11',50,floor(aa[1]/aa[2]))
  for(ii in 1:Number_repeat){
    no_index_select <- sample(no_index,size = aa[2])
    select_index <- (1:length(dat_analysis1$Response)) %in% union(no_index_select,which((dat_analysis1$Response)=='1'))
    RF_model <- randomForest(Response~.,data=dat_analysis1[select_index,],ntree=20)
    Model_prob.target <- Model_prob.target + predict(RF_model,newdata=dat_target[-1]) %>>% as.character %>>% as.numeric 
    Model_prob.efficiency <- Model_prob.efficiency + predict(RF_model,newdata=dat_analysis[-79]) %>>% as.character %>>% as.numeric
    cat('ii=',ii,'\n')}
  Result_userPredict <- data.frame(userid=dat_target$userid,Predict=ifelse(Model_prob.target==Number_repeat,1,0))
  ConfusionMatrix <- data.frame(real = as.character(dat_analysis$Response),fit = ifelse(Model_prob.efficiency==Number_repeat,"1","0")) %>>% count(real,fit)
  return(list(storeid = i,UserPredict = Result_userPredict,ModelEfficiency = ConfusionMatrix,columnMatch = all.equal(names(dat_target)[2:79],names(dat_analysis)[1:78])))
})
qq <- summary_result %>>% llply(function(x) {
  dd1 <- ifelse(as.numeric(x$storeid) %in% c(1:9),paste('0',x$storeid,sep=''),x$storeid)
  dd <- x$UserPredict %>>% setnames(names(.),c('userid',paste('store',dd1,sep=''))) %>>% transform(userid=as.character(userid))
  return(dd)})
Predict_result <- merge(qq[[1]],qq[[2]],by='userid',all.x=TRUE,all.y=TRUE) 
for(i in 3:13){
  Predict_result <- merge(Predict_result,qq[[i]],by='userid',all.x=TRUE,all.y=TRUE)
}
Predict_result <- Predict_result %>>% lapply(function(x){x[is.na(x)]=0 
return(x)}) %>>% as.data.frame %>>% 
  transform(userid = as.character(userid)) %>>% 
  mutate(nn = store01+store02+store03+store04+store05+store06+store07+store08+store09+store10+store11+store12+store13) %>>% 
  filter(nn>0) %>>% dplyr::select(-nn)
OutputText <- as.character()
for(i in 1:length(Predict_result$userid)){
  Variable <- c(1:13)
  Store <- paste(Variable[Predict_result[i,2:14]!=0],collapse = ',')
  OutputText[i] <- paste(Predict_result[i,'userid'],'^',Store,sep='')}
fileConn <- file("output.txt")
paste(OutputText,collapse='\n') %>>% iconv(from='UTF-8',to='BIG5') %>>%
  writeLines(fileConn)
close(fileConn)









