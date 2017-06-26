use mif;
create table feesOutliers(
  person_number varchar(100),        /*个人编号*/
  working_retired varchar(10),                 /*在职离退状态*/
  workplace varchar(100),            /*单位性质*/
  age int,                      /*年龄*/
  sex varchar(2),            /*性别*/
  wage float,           /*年度工资*/
  hospital_number varchar(100),      /*医疗机构代码*/
  grade varchar(100),   /*医院等级*/
  days int,         /*住院天数*/
  drugfees float,   /*药品费*/
  line float,    /*起付线*/
  ratio float,    /*报销比例*/
  chroric char(2),   /*是否患有慢性病*/
  inHospital date,   /*入院日期*/
  outHospital date,  /*出院日期*/
  disease varchar(100),  /*病种*/
  trementfees float,    /*诊疗费*/
  bedfees float,      /*床位费*/
  operationfees float,  /*手术费*/
  carefees float,   /*护理费*/
  materialfees float,  /*材料费*/
  groupfees float,     /*统筹费用支出*/
  state   varchar(100)   /*状态*/
);
load data local infile "C:\\Users\\song\\Desktop\\feesOutliers.csv" into table feesOutliers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 