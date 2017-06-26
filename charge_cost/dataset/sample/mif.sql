use mif;
create table group_income(
  year Year,
  avgWage double,
  working int,
  retired int);
load data local infile 'F:\\project\\mif\\charge_cost\\dataset\\group_income.txt' into table group_income fields TERMINATED BY '\t' LINES TERMINATED BY '\n' ;  