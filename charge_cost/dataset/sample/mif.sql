use mif;
create table group_income(
  year Year,
  avgWage double,
  working int,
  retired int);
load data local infile 'F:\\project\\mif\\charge_cost\\dataset\\group_income.txt' into table group_income fields TERMINATED BY '\t' LINES TERMINATED BY '\n';

alter table cost add column menzhen double;
insert into cost(menzhen) values(61758203);
insert into cost(menzhen) values(72968965);
insert into cost(menzhen) values(84179742);
insert into cost(menzhen) values(95390519);
set sql_safe_updates=0;
delete from cost where year is null;
update cost set menzhen=61758203 where year='2016';
update cost set menzhen=72968965 where year='2017';
update cost set menzhen=84179742 where year='2018';
update cost set menzhen=95390519 where year='2019';
update cost set menzhen=106601282 where year='2020'