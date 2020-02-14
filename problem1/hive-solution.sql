create table product(productId int, price string, saleEvent string, rivalName string, fetchTS string) row format delimited fields terminated by '|' stored as textfile;
load data local inpath '/usr/local/ct/Rajesh/Hive/HiveDataProduct.csv' into table product;

create table partition_prodcut (productId int, price string, saleEvent string, fetchTS string) partitioned by (rivalName string);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.enforce.bucketing =true;

insert overwrite table partition_prodcut partition(rivalName) select productId, price, saleEvent, fetchTS,translate(rivalName,".","_")  from product;