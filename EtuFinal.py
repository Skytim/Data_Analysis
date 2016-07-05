from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("EtuData").setMaster("local[4]")
sc = SparkContext(conf=conf)

order= sc.textFile("EtuData/order_step2.csv").zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(",")).map(lambda y:y[0]+","+y[1]+","+y[2]+","+y[5]+","+y[6]+","+y[4][0:4]+"-"+y[4][4:6]+"-"+y[4][6:8]+","+y[0]+"."+y[1]+"."+y[2])
text_file = open("order2.csv", "w")
text_file.write("storeid,catid_1,catid_2,amt,userid,dates,target_id"+"\n")
for ct in order.collect():
    text_file.write(ct+"\n")
text_file.close()
view= sc.textFile("EtuData/view_step2.csv").zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(",")).map(lambda y:y[0]+","+y[2]+","+y[3]+","+y[4]+","+y[5]+","+y[2]+"."+y[3]+"."+y[4])
view_file = open("view2.csv", "w")
view_file.write("dates,storeid,catid_1,catid_2,userid,target_id"+"\n")
for ct in view.collect():
    view_file.write(ct+"\n")
view_file.close()