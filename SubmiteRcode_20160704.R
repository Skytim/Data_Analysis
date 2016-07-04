library(data.table)
library(tidyr)
library(plyr)
library(dplyr)
library(pipeR)
library(randomForest)

AdjustNumber <- 2
NumberTree <- 30 
# load data
dat_order <- readLines(file('orderRefine.csv',encoding = "UTF-8")) %>>% strsplit(split=',') %>>% 
  (do.call(rbind,.)) %>>% (~a=.[1,]) %>>% tbl_dt(F) %>>% setnames(names(.),a) %>>% 
  (.[-1,]) %>>% mutate(amt=as.numeric(amt),dates=as.Date(dates,format='%Y-%m-%d'))

dat_view <- readLines(file('viewRefine.csv',encoding = "UTF-8")) %>>% strsplit(split=',') %>>% 
  (do.call(rbind,.)) %>>% (~a=.[1,]) %>>% tbl_dt(F) %>>% setnames(names(.),a) %>>% 
  (.[-1,]) %>>% mutate(dates=as.Date(dates,format='%Y-%m-%d'))


dat_subsidiary <- rbind(dat_order %>>% tidyr::expand(tidyr::nesting(storeid,catid_1,catid_2)),
                        dat_view %>>% tidyr::expand(tidyr::nesting(storeid,catid_1,catid_2))) %>>% 
  dplyr::distinct(storeid,catid_1,catid_2) %>>% 
  mutate(target_id = paste(storeid,catid_1,catid_2,sep='.')) %>>% 
  tbl_dt(F) %>>% select(storeid,catid_1,catid_2,target_id) %>>% 
  tidyr::expand(tidyr::nesting(storeid,catid_1,catid_2,target_id),
                dates=as.Date(paste('2015-',6:10,'-1',sep=''))) %>>%
  mutate(userid='subsidiary')
dat_order <- rbind(dat_order, dat_subsidiary %>>% mutate(amt=0) %>>% 
                     select(storeid,catid_1,catid_2,amt,userid,dates,target_id)) %>>% tbl_dt(F)
dat_view <- rbind(dat_view, dat_subsidiary %>>% 
                    select(dates,storeid,catid_1,catid_2,userid,target_id)) %>>% tbl_dt(F)
rm(dat_subsidiary)


Result_userPredict <- data.frame(userid=character(),Type=character()) %>>% mutate_each(funs(as.character))
# start alg.

for(ii in setdiff(as.character(1:13),'11')){
  
  for(i in 9:6){
    dat_order %>>% filter(month(dates) %in% c(i,i+1),storeid==ii) %>>% 
      (split(.,ifelse(month(.$dates)==i,'Covariate','Response'))) %>>%
      (~dat_orderCovariate = .$Covariate) %>>% 
      (~dat_orderResponse = .$Response)
    dat_view %>>% filter(month(dates) %in% c(i,i+1),storeid==ii) %>>%  
      (split(.,ifelse(month(.$dates)==i,'Covariate','Response'))) %>>%
      (~dat_viewCovariate = .$Covariate) 
    ############################################
    if(i==9){
      dat_orderCovariate <- dat_orderCovariate %>>% group_by(userid,target_id) %>>% summarise(amt=sum(amt)) %>>%
        setnames(names(.),c('userid','Order','amt')) %>>% ungroup() %>>% tbl_dt(F) %>>%
        tidyr::spread(key=Order,value=amt,fill=0,sep='_')  
      dat_viewCovariate <-  dat_viewCovariate %>>% group_by(userid,target_id) %>>% summarise(Number=n()) %>>%
        setnames(names(.),c('userid','View','Number')) %>>% ungroup() %>>% tbl_dt(F) %>>%
        tidyr::spread(key=View,value=Number,fill=0,sep='_')
      dat_analysis <- merge(dat_orderCovariate,dat_viewCovariate,by='userid',all.x=TRUE,all.y=TRUE) 
      dat_orderResponse <-  dat_orderResponse %>>% group_by(userid,target_id) %>>% summarise(R=1) %>>% 
        setnames(names(.),c('userid','Response','R')) %>>% ungroup() %>>% unique %>>%
        tbl_dt(F) %>>% tidyr::spread(key=Response,value=R,fill=0,sep='_')
      dat_analysis <- merge(dat_analysis,dat_orderResponse,by='userid',all.x=TRUE,all.y=TRUE) %>>% as.data.frame
      Name <- names(dat_analysis)
    }
    else{
      dat_orderCovariate <- dat_orderCovariate %>>% group_by(userid,target_id) %>>% summarise(amt=sum(amt)) %>>%
        setnames(names(.),c('userid','Order','amt')) %>>% ungroup() %>>% tbl_dt(F) %>>%
        tidyr::spread(key=Order,value=amt,fill=0,sep='_')  
      dat_viewCovariate <-  dat_viewCovariate %>>% group_by(userid,target_id) %>>% summarise(Number=n()) %>>%
        setnames(names(.),c('userid','View','Number')) %>>% ungroup() %>>% tbl_dt(F) %>>%
        tidyr::spread(key=View,value=Number,fill=0,sep='_')
      dat_analysis1 <- merge(dat_orderCovariate,dat_viewCovariate,by='userid',all.x=TRUE,all.y=TRUE) 
      dat_orderResponse <-  dat_orderResponse %>>% group_by(userid,target_id) %>>% summarise(R=1) %>>% 
        setnames(names(.),c('userid','Response','R')) %>>% ungroup() %>>% unique %>>%
        tbl_dt(F) %>>% tidyr::spread(key=Response,value=R,fill=0,sep='_')
      dat_analysis1 <- merge(dat_analysis1,dat_orderResponse,by='userid',all.x=TRUE,all.y=TRUE) %>>% as.data.frame
      dat_analysis1 <- dat_analysis1[Name]
      dat_analysis <- rbind(dat_analysis,dat_analysis1)
    }
    cat('i=',i,'\n')
  }
  
  
  
  ## target
  i = 10
  
  dat_orderCovariate <- dat_order %>>% filter(month(dates)==i,storeid==ii) %>>% group_by(userid,target_id) %>>% 
    summarise(amt=sum(amt)) %>>% setnames(names(.),c('userid','Order','amt')) %>>% ungroup() %>>% tbl_dt(F) %>>%
    tidyr::spread(key=Order,value=amt,fill=0,sep='_')
  dat_viewCovariate <- dat_view %>>% filter(month(dates)==i,storeid==ii) %>>% group_by(userid,target_id) %>>% 
    summarise(Number=n()) %>>% setnames(names(.),c('userid','View','Number')) %>>% ungroup() %>>% tbl_dt(F) %>>%
    tidyr::spread(key=View,value=Number,fill=0,sep='_')
  dat_target <- merge(dat_orderCovariate,dat_viewCovariate,by='userid',all.x=TRUE,all.y=TRUE) %>>% as.data.frame
  dat_analysis <- dat_analysis %>>% mutate_each(funs(mapvalues(.,NA,0)),-userid) %>>% filter(userid!='subsidiary')
  dat_target <- dat_target %>>% mutate_each(funs(mapvalues(.,NA,0)),-userid) %>>% filter(userid!='subsidiary')
  cc <- setdiff(names(dat_analysis),names(dat_target))
  
  for(k in 1:length(cc)){
    
    dat_analysis1 <- dat_analysis[c(setdiff(Name,cc),cc[k])] %>>% 
      setnames(names(.),c(setdiff(Name,cc),'Response')) %>>% 
      select(-userid)  %>>% mutate(tt = ifelse(Response==1,rnorm(1),0)) %>>% 
      distinct %>>% select(-tt)
    
    Number_repeat <- ifelse(sum((dat_analysis1$Response)==1)==0,
                            0,floor(sum(dat_analysis1$Response==0)/sum(dat_analysis1$Response==1)))
    
    
    if(Number_repeat!=0){
      
      no_index <- setdiff(1:length(dat_analysis1$Response),which(dat_analysis1$Response==1))
      Number_repeat1 <- sum(dat_analysis1$Response==1)
      Model_prob.target <- rep(0,length(dat_target$userid))    
      dat_analysis1$Response <- as.factor(dat_analysis1$Response)
      if(Number_repeat<=100){
        for(iii in 1:Number_repeat){
          no_index_select <- sample(no_index,size = Number_repeat1)
          select_index <- (1:length(dat_analysis1$Response)) %in% union(no_index_select,which((dat_analysis1$Response)=='1'))
          RF_model <- randomForest(Response~.,data=dat_analysis1[select_index,],ntree=20)
          Model_prob.target <- Model_prob.target + predict(RF_model,newdata=dat_target%>>% (.[setdiff(names(dat_analysis1),'Response')])) %>>% as.character %>>% as.numeric 
          cat('iii=',iii,'\n')
        }
        Result_userPredict <- rbind(dat_target %>>% select(userid) %>>% mutate(Predict=ifelse(Model_prob.target==Number_repeat,1,0)) %>>% 
                                      filter(Predict==1) %>>% select(userid) %>>% mutate(Type=substr(cc[k],start=10,stop=nchar(cc[k]))),
                                    Result_userPredict)
      }
      else{}
    } 
    else{}
    cat('k=',k,'/',length(cc),'\n')
  }
  
  cat('ii=',ii,'\n')
}

Result_userPredict <- Result_userPredict %>>% distinct %>>% tbl_dt(F)

AA <- rbind(dat_order %>>% select(userid,target_id),dat_view %>>% select(userid,target_id)) %>>% count(userid,target_id) %>>% 
  tbl_dt(F) %>>% filter(n>AdjustNumber) %>>% select(userid,target_id) %>>% setnames(names(.),c('userid','Type'))


Result_userPredict <- merge(Result_userPredict,AA,by=c('userid','Type'),all.x = F,all.y = F)


A<- Result_userPredict$Type %>>% (gsub('\\.',replacement = '-',.)) %>>% split(Result_userPredict$userid) %>>% 
  llply(function(x){paste0(x,collapse=',')}) %>>% (do.call(rbind,.))

r<-attributes(A)[[2]][[1]]
Result <- data.frame(userid=r,predict=A[,1],stringsAsFactors=F) %>>% tbl_dt(F) %>>%
  mutate(result=paste(userid,predict,sep='^'))

fileConn<- format(Sys.time(), "%Y%m%d%H%M%S") %>>% as.character %>>% (paste('result_',.,'.txt',sep='')) %>>% file()
paste(Result$result,collapse='\n') %>>% iconv(from='UTF-8',to='BIG5') %>>%
  writeLines(fileConn)
close(fileConn)



