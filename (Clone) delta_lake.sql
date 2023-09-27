-- Databricks notebook source
create schema sample3

-- COMMAND ----------

Create table emppp(id int, name string, age int, dept string) location "dbfs:/mnt/praketstorage/raw/delta/praket/emp"

-- COMMAND ----------

describe extended emppp

-- COMMAND ----------

describe history emppp

-- COMMAND ----------

select * from emppp

-- COMMAND ----------

insert into table emppp values(1,'a',23,'DE')

-- COMMAND ----------

select * from emppp

-- COMMAND ----------

describe history emppp

-- COMMAND ----------

insert into table emppp values(2,'b',23,'DE')

-- COMMAND ----------

insert into table emppp values(3,'c',23,'DE'),
                            (4,'d',23,'DE')

-- COMMAND ----------

describe history emppp

-- COMMAND ----------

select * from emppp

-- COMMAND ----------

delete from emppp where id= 1

-- COMMAND ----------

select * from emppp

-- COMMAND ----------

Update emppp
set dept='DS'
where id= 4

-- COMMAND ----------

describe history emppp

-- COMMAND ----------

select * from emppp

-- COMMAND ----------

select * from emppp version as of 3

-- COMMAND ----------

create table oldemp2 as 
select * from emppp version as of 3

-- COMMAND ----------

select * from emppp timestamp as of '2023-09-27T08:43:01.000+0000'

-- COMMAND ----------


