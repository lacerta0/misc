-----------------------------------------
--ПОДГОТОВКА БАЗЫ
-----------------------------------------

CREATE TABLE TMP_SLV_FOR_CS_MODEL AS
select t.idblank_tm as idblank,
t.contact_call_time_start,
b.datenter
from tmp_ag_fun_cs_11 t

inner join agr_blanks b
  on t.idblank_tm = b.idblank

inner join LIB_PRODUCT_HIERARCHY c
  on b.idcardtype = c.prod_card_type_id
  and c.prod_grp_id = 100000022
  and c.prod_type_id = 100000060

where t.idblank_cis = t.idblank_tm
      and t.imported_at >= t.date_out_cis
      and t.call_time_start >= t.imported_at
      and t.contact_call_time_start >= t.call_time_start
      and trunc(t.imported_at - t.date_out_cis) <= 10
;

--drop table TMP_SLV_FOR_CS_MODEL2
CREATE TABLE TMP_SLV_FOR_CS_MODEL2 AS
select
  a.idblank,
  trunc(min(a.contact_call_time_start)) as contact_dt
-----!!!!!!!!!!!!!!!!!!!!!!!!!!(1)
from TMP_SLV_FOR_CS_MODEL a
group by a.idblank;

--drop table TMP_SLV_FOR_CS_MODEL3
CREATE TABLE TMP_SLV_FOR_CS_MODEL3 AS
select
  a.idblank,
  a.contact_dt,
  b.datfcretxn

from TMP_SLV_FOR_CS_MODEL2 a

left join CRM_CARDS_FCRED_TXN b
  on a.idblank = b.idblank
;


delete from TMP_SLV_FOR_CS_MODEL3 a where a.contact_dt > date'2017-11-11';
delete from TMP_SLV_FOR_CS_MODEL3 a where a.datfcretxn < a.contact_dt;


--drop table TMP_SLV_FOR_CS_MODEL4
CREATE TABLE TMP_SLV_FOR_CS_MODEL4 AS
select
  b.idclient,
  a.idblank,
  a.contact_dt,
  case when a.datfcretxn between a.contact_dt and (a.contact_dt + 30) then 1 else 0 end as event

from TMP_SLV_FOR_CS_MODEL3 a

inner join AGR_BLANKS b
  on a.idblank = b.idblank
;

--drop table TMP_SLV_FOR_CS_MODEL5
CREATE TABLE TMP_SLV_FOR_CS_MODEL5_11 AS
select
  a.*,
  b.segm

from TMP_SLV_FOR_CS_MODEL4 a

inner join AGR_CLIENT_PROD_SEGMENT b
  on a.idclient = b.idclient
  and a.contact_dt between b.effective_from_dt and b.effective_to_dt
--  and b.segm between 1 and 7 ---credit
    and b.segm between 8 and 11
;

--select contact_dt, count(*), sum(event) from TMP_SLV_FOR_CS_MODEL5 group by contact_dt


-----------------------------------------
--БАЗА
-----------------------------------------

--drop table SLV_CS_NEW_BASE
CREATE TABLE SLV_CS_NEW_BASE AS
select
  a.* 
from TMP_SLV_FOR_CS_MODEL5 a
;
select segm,contact_dt,sum(event),count(1) from SLV_CS_NEW_BASE
group by segm, contact_dt
-----------------------------------------
--БЛОК СБОРКИ ВЕКТОРА
-----------------------------------------


---------------------------------------------------------------------------------------------
--Оформленные POS и PIL
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S1
CREATE TABLE TMP_SLV_CS_NEW_S1 AS 
SELECT
    a.idclient,
    a.idblank,
    a.contact_dt,
    1 as pp_hist_flg,
    max(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as pp_curr_flg,
    max(case when c.prod_cat_id = 100000001 then 1 else 0 end) as pos_hist_flg,
    max(case when c.prod_cat_id = 100000004  then 1 else 0 end) as pil_hist_flg,
    max(case when c.prod_cat_id = 100000001 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as pos_curr_flg,
    max(case when c.prod_cat_id = 100000004 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as pil_curr_flg,
    sum(1) as pp_hist_cnt,
    sum(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as pp_curr_cnt,
    sum(case when c.prod_cat_id = 100000001 then 1 else 0 end) as pos_hist_cnt,
    sum(case when c.prod_cat_id = 100000004  then 1 else 0 end) as pil_hist_cnt,
    sum(case when c.prod_cat_id = 100000001 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as pos_curr_cnt,
    sum(case when c.prod_cat_id = 100000004 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as pil_curr_cnt,
    trunc(months_between(a.contact_dt, min(case when c.prod_cat_id = 100000001 then b.datestart end))) as mnth_from_first_pos,
    trunc(months_between(a.contact_dt, max(case when c.prod_cat_id = 100000001 then b.datestart end))) as mnth_from_last_pos,
    trunc(months_between(a.contact_dt, min(case when c.prod_cat_id = 100000004 then b.datestart end))) as mnth_from_first_pil,
    trunc(months_between(a.contact_dt, max(case when c.prod_cat_id = 100000004 then b.datestart end))) as mnth_from_last_pil,
    trunc(months_between(a.contact_dt, min(b.datestart))) as mnth_from_first_pp,
    trunc(months_between(a.contact_dt, max(b.datestart))) as mnth_from_last_pp

from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b
      on a.idclient = b.idclient
      and b.datestart is not null
      and b.datestart < a.contact_dt

inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
          on b.idcardtype = c.prod_card_type_id
          and c.prod_cat_id in (100000001, 100000004)
group by a.idclient, a.idblank, a.contact_dt
;         


---------------------------------------------------------------------------------------------
--Оформленные карты
---------------------------------------------------------------------------------------------

--drop table TMP_SLV_CS_NEW_S2
CREATE TABLE TMP_SLV_CS_NEW_S2 AS
SELECT
    a.idclient,
    a.idblank,
    a.contact_dt,
    1 as card_hist_flg,
    max(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as card_curr_flg,
    max(case when c.prod_grp_id = 100000022 then 1 else 0 end) as cc_hist_flg,
    max(case when c.prod_grp_id != 100000022  then 1 else 0 end) as dc_hist_flg,
    max(case when c.prod_grp_id = 100000022 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as cc_curr_flg,
    max(case when c.prod_grp_id != 100000022 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as dc_curr_flg,
    sum(1) as card_hist_cnt,
    sum(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as card_curr_cnt,
    sum(case when c.prod_grp_id = 100000022 then 1 else 0 end) as cc_hist_cnt,
    sum(case when c.prod_grp_id != 100000022  then 1 else 0 end) as dc_hist_cnt,
    sum(case when c.prod_grp_id = 100000022 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as cc_curr_cnt,
    sum(case when c.prod_grp_id != 100000022 and coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as dc_curr_cnt,
    trunc(months_between(a.contact_dt, min(case when c.prod_grp_id = 100000022 then b.datestart end))) as mnth_from_first_cc,
    trunc(months_between(a.contact_dt, max(case when c.prod_grp_id = 100000022 then b.datestart end))) as mnth_from_last_cc,
    trunc(months_between(a.contact_dt, min(case when c.prod_grp_id != 100000022 then b.datestart end))) as mnth_from_first_dc,
    trunc(months_between(a.contact_dt, max(case when c.prod_grp_id != 100000022 then b.datestart end))) as mnth_from_last_dc,
    trunc(months_between(a.contact_dt, min(b.datestart))) as mnth_from_first_card,
    trunc(months_between(a.contact_dt, max(b.datestart))) as mnth_from_last_card
    
from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b
      on a.idclient = b.idclient
      and b.base_supp = '1'
      and b.idblank != a.idblank
      and b.datestart is not null
      and b.datestart < a.contact_dt 

inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
      on b.idcardtype = c.prod_card_type_id
      and c.prod_cat_id = 100000005 

group by a.idclient, a.idblank, a.contact_dt
;

---------------------------------------------------------------------------------------------
--Реструктуризации и страховки
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S3
CREATE TABLE TMP_SLV_CS_NEW_S3 AS
SELECT
    a.idclient,
    a.idblank,
    a.contact_dt,
    max(case when (c.prod_cat_id = 100001843) and (b.datestart is not null) then 1 else 0 end) as restr_hist_flg,
    max(case when (c.prod_cat_id = 100001843) and (b.datestart is not null) and (coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt) then 1 else 0 end) as restr_curr_flg,--реструктуризация действующая
    max(case when (c.prod_cat_id = 100001655) and (b.datestart is not null) then 1 else 0 end) as insur_hist_flg,
    max(case when (c.prod_cat_id = 100001655) and (b.datestart is not null) and (coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt) then 1 else 0 end) as insur_curr_flg,
    sum(case when (c.prod_cat_id = 100001843) and (b.datestart is not null) then 1 else 0 end) as restr_hist_cnt,
    sum(case when (c.prod_cat_id = 100001843) and (b.datestart is not null) and (coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt) then 1 else 0 end) as restr_curr_cnt,
    sum(case when (c.prod_cat_id = 100001655) and (b.datestart is not null) then 1 else 0 end) as insur_hist_cnt,
    sum(case when (c.prod_cat_id = 100001655) and (b.datestart is not null) and (coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt) then 1 else 0 end) as insur_curr_cnt,
    trunc(months_between(a.contact_dt, min(case when c.prod_cat_id = 100001843 then b.datestart end))) as mnth_from_first_restr,
    trunc(months_between(a.contact_dt, max(case when c.prod_cat_id = 100001843 then b.datestart end))) as mnth_from_last_restr,
    trunc(months_between(a.contact_dt, min(case when c.prod_cat_id = 100001655 then b.datestart end))) as mnth_from_first_insur,
    trunc(months_between(a.contact_dt, max(case when c.prod_cat_id = 100001655 then b.datestart end))) as mnth_from_last_insur

from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b
      on a.idclient = b.idclient
      and b.datestart < a.contact_dt

inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
      on b.idcardtype = c.prod_card_type_id
      and c.prod_cat_id in (100001655, 100001843)
group by a.idclient, a.idblank, a.contact_dt
;

---------------------------------------------------------------------------------------------
--Векторные переменные по кредитным картам изменение числа кредитных карт за последние 3,6,12 мес
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S4
CREATE TABLE TMP_SLV_CS_NEW_S4 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  cast(cnt_curr_cc as decimal(18,2))/(nvl(cnt_curr_cc_3,0)+cnt_curr_cc)*2.0 as cnt_act_cc_3v,
  cast(cnt_curr_cc as decimal(18,2))/(nvl(cnt_curr_cc_6,0)+cnt_curr_cc)*2.0 as cnt_act_cc_6v,
  cast(cnt_curr_cc as decimal(18,2))/(nvl(cnt_curr_cc_12,0)+cnt_curr_cc)*2.0 as cnt_act_cc_12v
from (SELECT  
        a.idclient,
        a.idblank,
        a.contact_dt,
        sum(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as cnt_curr_cc,
        sum(case when b.datestart < add_months(a.contact_dt,-3) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-3) then 1 else 0 end) as cnt_curr_cc_3,
        sum(case when b.datestart < add_months(a.contact_dt,-6) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-6) then 1 else 0 end) as cnt_curr_cc_6,
        sum(case when b.datestart < add_months(a.contact_dt,-12) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-12) then 1 else 0 end) as cnt_curr_cc_12
     
       from SLV_CS_NEW_BASE a
            
       inner join AGR_BLANKS b 
         on a.idclient = b.idclient
         and b.base_supp = '1'
         and a.idblank != b.idblank--исключение случаев транзакций по карте без коммуникаций
         and b.datestart is not null
         and b.datestart < a.contact_dt 
          
       inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
         on b.idcardtype = c.prod_card_type_id
         and c.prod_grp_id = 100000022 
      
       group by a.idclient, a.idblank, a.contact_dt
      ) a
where cnt_curr_cc>0 
;

--------------------------------------------------------------------------------------------
--Векторные переменные по дебетовым картам
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S5
CREATE TABLE TMP_SLV_CS_NEW_S5 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  cast(cnt_curr_dc as decimal(18,2))/(nvl(cnt_curr_dc_3,0)+cnt_curr_dc)*2.0 as cnt_act_dc_3v,
  cast(cnt_curr_dc as decimal(18,2))/(nvl(cnt_curr_dc_6,0)+cnt_curr_dc)*2.0 as cnt_act_dc_6v,
  cast(cnt_curr_dc as decimal(18,2))/(nvl(cnt_curr_dc_12,0)+cnt_curr_dc)*2.0 as cnt_act_dc_12v
from (SELECT  
        a.idclient,
        a.idblank,
        a.contact_dt,
        sum(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as cnt_curr_dc,
        sum(case when b.datestart < add_months(a.contact_dt,-3) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-3) then 1 else 0 end) as cnt_curr_dc_3,
        sum(case when b.datestart < add_months(a.contact_dt,-6) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-6) then 1 else 0 end) as cnt_curr_dc_6,
        sum(case when b.datestart < add_months(a.contact_dt,-12) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-12) then 1 else 0 end) as cnt_curr_dc_12
     
       from SLV_CS_NEW_BASE a
              
       inner join AGR_BLANKS b 
         on a.idclient = b.idclient
         and b.base_supp = '1'
         and a.idblank != b.idblank
         and b.datestart is not null
         and b.datestart < a.contact_dt
          
       inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
         on b.idcardtype = c.prod_card_type_id
         and c.prod_cat_id = 100000005 
         and c.prod_grp_id != 100000022 
      
       group by a.idclient, a.idblank, a.contact_dt
      ) a
where cnt_curr_dc > 0 
;

---------------------------------------------------------------------------------------------
--Векторные переменные по POS-ам
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S6
CREATE TABLE TMP_SLV_CS_NEW_S6 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  cast(cnt_curr_pos as decimal(18,2))/(nvl(cnt_curr_pos_3,0)+cnt_curr_pos)*2.0 as cnt_act_pos_3v,
  cast(cnt_curr_pos as decimal(18,2))/(nvl(cnt_curr_pos_6,0)+cnt_curr_pos)*2.0 as cnt_act_pos_6v,
  cast(cnt_curr_pos as decimal(18,2))/(nvl(cnt_curr_pos_12,0)+cnt_curr_pos)*2.0 as cnt_act_pos_12v
from (SELECT  
        a.idclient,
        a.idblank,
        a.contact_dt,
        sum(case when coalesce(trunc(b.datexit), trunc(sysdate)) > a.contact_dt then 1 else 0 end) as cnt_curr_pos,
        sum(case when datestart < add_months(a.contact_dt,-3) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-3) then 1 else 0 end) as cnt_curr_pos_3,
        sum(case when datestart < add_months(a.contact_dt,-6) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-6) then 1 else 0 end) as cnt_curr_pos_6,
        sum(case when datestart < add_months(a.contact_dt,-12) and coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-12) then 1 else 0 end) as cnt_curr_pos_12
     
       from SLV_CS_NEW_BASE a
            
       inner join AGR_BLANKS b 
         on a.idclient = b.idclient
         and a.idblank != b.idblank
         and b.datestart is not null
         and b.datestart < a.contact_dt
          
       inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
         on b.idcardtype = c.prod_card_type_id
         and c.prod_cat_id = 100000001
      
       group by a.idclient, a.idblank, a.contact_dt
      ) a
where cnt_curr_pos>0 
;


---------------------------------------------------------------------------------------------
--Данные об архивных продуктах
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S7
CREATE TABLE TMP_SLV_CS_NEW_S7 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  cnt_arc_prod,
  cast(cnt_arc_prod as decimal(18,2))/(cnt_arc_prod+nvl(cnt_arc_prod_3,0))*2.0  as cnt_arc_prod_v3,
  cast(cnt_arc_prod as decimal(18,2))/(cnt_arc_prod+nvl(cnt_arc_prod_6,0))*2.0 as cnt_arc_prod_v6,
  cast(cnt_arc_prod as decimal(18,2))/(cnt_arc_prod+nvl(cnt_arc_prod_12,0))*2.0 as cnt_arc_prod_v12
from (SELECT  
         a.idclient, 
         a.idblank,
        a.contact_dt,
        sum(case when coalesce(trunc(b.datexit), trunc(sysdate)) < a.contact_dt then 1 else 0 end) as cnt_arc_prod,
        sum(case when (b.datestart < add_months(a.contact_dt,-3)) 
                      and coalesce(trunc(b.datexit), trunc(sysdate)) < add_months(a.contact_dt,-3) then 1 else 0 end) as cnt_arc_prod_3,
        sum(case when (b.datestart < add_months(a.contact_dt,-6)) 
                      and coalesce(trunc(b.datexit), trunc(sysdate)) < add_months(a.contact_dt,-6) then 1 else 0 end) as cnt_arc_prod_6,
        sum(case when (b.datestart < add_months(a.contact_dt,-12)) 
                      and coalesce(trunc(b.datexit), trunc(sysdate)) < add_months(a.contact_dt,-12) then 1 else 0 end) as cnt_arc_prod_12                      
      
        from SLV_CS_NEW_BASE a
           
        inner join AGR_BLANKS b 
          on a.idclient = b.idclient
          and a.idblank != b.idblank
          and b.datestart < a.contact_dt
      
        inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
          on b.idcardtype = c.prod_card_type_id
          and c.prod_cat_id != 100000007
          
        group by a.idclient, a.idblank, a.contact_dt 
      ) a
where cnt_arc_prod > 0     
;


---------------------------------------------------------------------------------------------
--Расходные транзакции за последние три года помесячно
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S8
select * from TMP_SLV_CS_NEW_S8 where txn_amt = 0
CREATE TABLE TMP_SLV_CS_NEW_S8 AS
SELECT 
  a.idclient,
  a.idblank,
  a.contact_dt,
  c.prod_grp_id,
  trunc(d.txn_fact_dt,'mm') as month_begin,
  sum(d.sum_amt_rur) as txn_amt,
  sum(case when d.tran_group_code between 1 and 7 then sum_amt_rur end) as cash_amt,-- cash б-т и переводы
  sum(case when d.tran_group_code in (8,9,10) then sum_amt_rur end ) as pos_amt,
  sum(case when d.tran_group_code between 1 and 7 then 1 else 0 end) as cash_cnt,
  max(case when cntr_id in (638,639,186) or (cntr_id is null and c.prod_mark_grp_name <> 'Diners Cards')
                       then 0 else 1 end) as txn_foreign_flg,
--cнятие cash в наших банкоматах
  max(case when tran_group_code = 1 and batch_id is not null then 1 else 0 end) as cash_rsb_flg,
--снятие cash в чужих банкоматах 
  max(case when tran_group_code = 1 and batch_id is null then 1 else 0 end) as cash_no_rsb_flg                     

  from SLV_CS_NEW_BASE a
       
  inner join AGR_BLANKS b 
    on a.idclient = b.idclient
    and a.idblank != b.idblank
    and b.datestart < a.contact_dt
      
  inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
    on b.idcardtype = c.prod_card_type_id
    and c.prod_cat_id = 100000005

  inner join AGR_CARD_TRXN d
        on b.idblank = d.idblank
        and d.txn_fact_dt >= add_months(a.contact_dt,-36)
        and d.txn_fact_dt <  a.contact_dt
        and d.tran_group_code between 1 and 10-- расх тр-и
 
 group by a.idclient, a.idblank, a.contact_dt, c.prod_grp_id, trunc(d.txn_fact_dt,'mm')
 ;



---------------------------------------------------------------------------------------------
--Транзакционное поведение по клиенту
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S9
CREATE TABLE TMP_SLV_CS_NEW_S9 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  case when sum(nvl(txn_amt,0))=0 then 'No TXN'
        when sum(cash_amt)>0.95*sum(txn_amt) then 'PURE CASHER'
              when sum(cash_amt)>sum(pos_amt) then 'CASH_TRANSFER' 
  ELSE 'POS' END AS cl_txn_type

from SLV_CS_NEW_BASE a

left join TMP_SLV_CS_NEW_S8 b
  on a.idclient = b.idclient
  and a.idblank = b.idblank
  and a.contact_dt = b.contact_dt

group by a.idclient, a.idblank, a.contact_dt
;
----------------------------------------------------------------------------------
--Агрегаты по расх тр-м (дополнение)
----------------------------------------------------------------------------------
CREATE TABLE TMP_SLV_CS_NEW_S22 AS
select b.idclient,
  b.idblank,
  b.contact_dt,
  b.txn_amt_1yr,
  b.txn_amt_2yr,
  b.txn_amt_3yr,
  (case txn_amt_2yr 
  when 0 then txn_amt_1yr/1 
  else txn_amt_1yr/nvl(txn_amt_2yr,1) end) as rel_txn_1_2,
  (case txn_amt_3yr 
  when 0 then txn_amt_1yr/1 
  else txn_amt_1yr/nvl(txn_amt_3yr,1) end) as rel_txn_1_3,
  (case txn_amt_3yr 
  when 0 then txn_amt_2yr/1 
  else txn_amt_2yr/nvl(txn_amt_3yr,1) end) rel_txn_2_3
  from 
      (SELECT
      a.idclient,
      a.idblank,
      a.contact_dt,
      sum(case when a.month_date_begin >= add_months(a.contact_dt,-12) then coalesce(t.txn_amt,0) else 0 end) as txn_amt_1yr,
      sum(case when a.month_date_begin >= add_months(a.contact_dt,-24) and a.month_date_begin < add_months(a.contact_dt,-12) then coalesce(t.txn_amt,0) else 0 end) as txn_amt_2yr,
      sum(case when a.month_date_begin >= add_months(a.contact_dt,-36) and a.month_date_begin < add_months(a.contact_dt,-24) then coalesce(t.txn_amt,0) else 0 end) as txn_amt_3yr
      
      from (SELECT 
              a.idclient,
              a.idblank,
              a.contact_dt,
              m.month_date_begin
            from SLV_CS_NEW_BASE a, (select distinct month_date_begin from CRM_USER.LIB_DATE)  m

             where  m.month_date_begin < a.contact_dt
             and m.month_date_begin >= add_months(a.contact_dt,-36)
           ) a
         
      left join TMP_SLV_CS_NEW_S8 t
          on a.idclient = t.idclient
          and a.idblank = t.idblank
          and a.contact_dt = t.contact_dt
          and a.month_date_begin = t.month_begin
    group by   a.idclient, a.idblank, a.contact_dt
    )b;
-----------------!!!!!!!!!!!!!!

---------------------------------------------------------------------------------------------
--Агрегаты по расходным транзакциям
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S10
CREATE TABLE TMP_SLV_CS_NEW_S10 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-3) and t.prod_grp_id = 100000022 then coalesce(t.pos_amt,0) else 0 end) as cc_pos_amt_3m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-6) and a.month_date_begin < add_months(a.contact_dt,-3) and t.prod_grp_id = 100000022 then coalesce(t.pos_amt,0) else 0 end) as cc_pos_amt_3_6m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6)  and t.prod_grp_id = 100000022 then coalesce(t.pos_amt,0) else 0 end) as cc_pos_amt_6_12m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-3) and t.prod_grp_id = 100000022 then coalesce(t.txn_amt,0) else 0 end) as cc_txn_amt_3m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-6) and a.month_date_begin < add_months(a.contact_dt,-3) and t.prod_grp_id = 100000022 then coalesce(t.txn_amt,0) else 0 end) as cc_txn_amt_3_6m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6) and t.prod_grp_id = 100000022 then coalesce(t.txn_amt,0) else 0 end) as cc_txn_amt_6_12m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-3) and t.prod_grp_id <> 100000022 then coalesce(t.pos_amt,0) else 0 end) as dc_pos_amt_3m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-6) and a.month_date_begin < add_months(a.contact_dt,-3) and t.prod_grp_id <> 100000022 then coalesce(t.pos_amt,0) else 0 end) as dc_pos_amt_3_6m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6)  and t.prod_grp_id <> 100000022 then coalesce(t.pos_amt,0) else 0 end) as dc_pos_amt_6_12m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-3) and t.prod_grp_id <> 100000022 then coalesce(t.txn_amt,0) else 0 end) as dc_txn_amt_3m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-6) and a.month_date_begin < add_months(a.contact_dt,-3) and t.prod_grp_id <> 100000022 then coalesce(t.txn_amt,0) else 0 end) as dc_txn_amt_3_6m,
  sum(case when a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6) and t.prod_grp_id <> 100000022 then coalesce(t.txn_amt,0) else 0 end) as dc_txn_amt_6_12m,
--- траты заграницей
  sum(case when t.txn_foreign_flg = 1 and a.month_date_begin >= add_months(a.contact_dt,-6) then 1 else 0 end) as foreign_txn_0_6m,
  sum(case when t.txn_foreign_flg = 1 and a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6) then 1 else 0 end) as foreign_txn_6_12m,
  sum(case when t.txn_foreign_flg = 1 and a.month_date_begin >= add_months(a.contact_dt,-18) and a.month_date_begin < add_months(a.contact_dt,-12) then 1 else 0 end) as foreign_txn_12_18m,
--- снятие cash в наших банкоматах
  sum(case when t.cash_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-6) and t.prod_grp_id = 100000022 then 1 else 0 end) as cc_cash_rsb_0_6m,
  sum(case when t.cash_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6) and t.prod_grp_id = 100000022 then 1 else 0 end) as cc_cash_rsb_6_12m,
  sum(case when t.cash_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-18) and a.month_date_begin < add_months(a.contact_dt,-12) and t.prod_grp_id = 100000022 then 1 else 0 end) as cc_cash_rsb_12_18m,
  sum(case when t.cash_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-6) and t.prod_grp_id <> 100000022 then 1 else 0 end) as dc_cash_rsb_0_6m,
  sum(case when t.cash_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6) and t.prod_grp_id <> 100000022 then 1 else 0 end) as dc_cash_rsb_6_12m,
  sum(case when t.cash_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-18) and a.month_date_begin < add_months(a.contact_dt,-12) and t.prod_grp_id <> 100000022 then 1 else 0 end) as dc_cash_rsb_12_18m,
--- снятие cash не в наших банкоматах
  sum(case when t.cash_no_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-6) then 1 else 0 end) as cash_no_rsb_0_6m,
  sum(case when t.cash_no_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-12) and a.month_date_begin < add_months(a.contact_dt,-6) then 1 else 0 end) as cash_no_rsb_6_12m,
  sum(case when t.cash_no_rsb_flg =1 and a.month_date_begin >= add_months(a.contact_dt,-18) and a.month_date_begin < add_months(a.contact_dt,-12)then 1 else 0 end) as cash_no_rsb_12_18m,  
--- кол-во мес. без расходных операций за 12 мес.
  sum(case when nvl(t.txn_amt,0)=0 and a.month_date_begin >= add_months(a.contact_dt,-12) then 1 else 0 end) as no_txn_12 

  from (SELECT 
          a.idclient,
          a.idblank,
          a.contact_dt,
          m.month_date_begin
        from SLV_CS_NEW_BASE a, (select distinct month_date_begin from CRM_USER.LIB_DATE)  m

         where  m.month_date_begin < a.contact_dt
         and m.month_date_begin >= add_months(a.contact_dt,-18)
       ) a
     
  left join TMP_SLV_CS_NEW_S8 t
      on a.idclient = t.idclient
      and a.idblank = t.idblank
      and a.contact_dt = t.contact_dt
      and a.month_date_begin = t.month_begin
group by   a.idclient, a.idblank, a.contact_dt
;


---------------------------------------------------------------------------------------------
--Доходные транзакции и разница в датах от транзакций
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S11
CREATE TABLE TMP_SLV_CS_NEW_S11 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  sum(case when tran_group_code > 10 and d.txn_fact_dt >= add_months(a.contact_dt, -12) and  d.txn_fact_dt < add_months(a.contact_dt, -6)  then sum_amt_rur end) as inc_amt_6_12m,
  trunc(months_between(a.contact_dt, min(case when tran_group_code > 10 then txn_fact_dt end))) as mnth_from_first_inc_txn,

  from SLV_CS_NEW_BASE a
       
  inner join AGR_BLANKS b 
    on a.idclient = b.idclient
    and a.idblank != b.idblank
    and b.datestart < a.contact_dt
      
  inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
    on b.idcardtype = c.prod_card_type_id
    and c.prod_cat_id = 100000005
  
  inner join AGR_CARD_TRXN d
        on b.idblank = d.idblank
        and d.txn_fact_dt <  a.contact_dt

group by   a.idclient, a.idblank, a.contact_dt
;


---------------------------------------------------------------------------------------------
--Добавляем продуктовую историю клиента в банке 
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S12
CREATE TABLE TMP_SLV_CS_NEW_S12 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  trunc(months_between(a.contact_dt, min(case when b.datestart < a.contact_dt then b.datestart end))) as mnth_from_first_prod,
  trunc(months_between(a.contact_dt, max(case when b.datestart < a.contact_dt then b.datestart end))) as mnth_from_last_prod,
  trunc(months_between(a.contact_dt, min(case when (c.prod_cat_id in (100000001, 100000002, 100000004, 100001843) or c.prod_grp_id = 100000022)
                                                   and b.datestart < a.contact_dt then b.datestart
                                         end))) as mnth_from_first_cred,  
  trunc(months_between(a.contact_dt, max(case when (c.prod_cat_id in (100000001, 100000002, 100000004, 100001843) or c.prod_grp_id = 100000022)
                                                   and b.datestart < a.contact_dt then b.datestart
                                         end))) as mnth_from_last_cred                                                                                                                       
                                         
from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b 
  on a.idclient = b.idclient
  and a.idblank != b.idblank
  and b.datenter < a.contact_dt

inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
  on b.idcardtype = c.prod_card_type_id
  and c.prod_cat_id != 100000007
  
group by a.idclient, a.idblank, a.contact_dt
;


---------------------------------------------------------------------------------------------
--Добавляем данные о заявках и отказах
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S13
CREATE TABLE TMP_SLV_CS_NEW_S13 AS
SELECT  
  a.idclient,
  a.idblank,
  a.contact_dt,
  trunc(months_between(a.contact_dt, max(b.datenter))) as mnth_from_last_entry,
  trunc(months_between(a.contact_dt, max(case when rezcr = -1 then b.datenter end))) as mnth_from_fail_entry,
  trunc(months_between(a.contact_dt, max(case when c.prod_grp_id = 100000022 or c.prod_cat_id in (100000001, 100000002, 100000004,  100000003, 100001843)  then b.datenter end))) as mnth_from_cred_entry,
  sum(case when b.rezcr = -1 and b.datenter >= add_months(a.contact_dt,-6) then 1 else 0 end) as cnt_entry_fail_6m,
  sum(case when b.datenter >= add_months(a.contact_dt,-6) then 1 else 0 end) as cnt_entry_6m,
  sum(case when c.prod_grp_id = 100000022 and b.rezcr = 1 then 1 else 0 end) as cnt_cc_entry_appr
    
from SLV_CS_NEW_BASE a
  
inner join AGR_BLANKS b 
  on a.idclient = b.idclient
  and a.idblank != b.idblank
  and b.datenter < a.contact_dt
      
inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
  on b.idcardtype = c.prod_card_type_id

group by a.idclient, a.idblank, a.contact_dt
;


---------------------------------------------------------------------------------------------
--Добавляем соцдем
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S14
CREATE TABLE TMP_SLV_CS_NEW_S14 AS
SELECT /*+ use_hash(a,b,c,d) */
  a.idclient,
  trunc((trunc(sysdate) -  (case when regexp_like(b.data_open,'[0-3][0-9]\W[0-1][0-9]\W[0-9][0-9][0-9][0-9]')
                              then to_date(b.data_open,'dd.mm.rrrr')
                         else to_date('01.01.1900','dd.mm.rrrr') end))/365) 
  as age,
  coalesce(b.pers1,d.income_stats) as income,
  b.education,
  b.work_otrasle,
  b.work_doljn,
  c.distance
  b.children,
  b.pol,
  b.married,
  b.naxlebnik
  (case when b.indok_pr_seria is not null then 1 else 0 end) as dr_licence
  
from SLV_CS_NEW_BASE a

inner join ORAWH.WH_CLIENTS b
  on a.idclient = b.idclient

inner join CRM_USER.VITR_CLIENTS c
  on a.idclient = c.idclient

left join priv_kd_mart.ukp_vitr_crm d
  on a.idclient = d.idclient
;

--------------------------------------------------------------------------------------------
--Добавляем данные о предложениях CS
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S15
CREATE TABLE TMP_SLV_CS_NEW_S15 AS
SELECT  
  a.idclient,
  a.idblank,
  a.contact_dt,
  count(*) as cs_cnt,
  max(case when b.datestart is not null and  b.datestart < a.contact_dt then 1 else 0 end) as cs_tkn_flg,
  sum(case when b.datestart is not null and  b.datestart < a.contact_dt then 1 else 0 end) as cs_tkn_cnt,
  sum(case when b.datestart is not null and  b.datestart < a.contact_dt and coalesce(b.datexit, sysdate) > a.contact_dt then 1 else 0 end) as cs_curr_cnt,
  trunc(months_between(a.contact_dt, min(b.datenter))) as mnth_from_first_cs,
  trunc(months_between(a.contact_dt, max(b.datenter))) as mnth_from_last_cs
  
from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b 
  on a.idclient = b.idclient
  and a.idblank != b.idblank
  and b.datenter < a.contact_dt
  and b.base_supp = '1'

inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
    on b.idcardtype = c.prod_card_type_id
    and c.prod_grp_id = 100000022
    and c.prod_sale_type_name = 'Cross Sale'

group by a.idclient, a.idblank, a.contact_dt
;

---------------------------------------------------------------------------------------------
--Депозиты
---------------------------------------------------------------------------------------------
--drop table CRM_USER.TMP_SLV_CS_S16
CREATE TABLE CRM_USER.TMP_SLV_CS_S16 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  dep_hist_cnt,
  dep_curr_cnt,
  dep_curr_amt,
  case when dep_curr_cnt = 0 then 0 else cast(dep_curr_cnt as decimal(18,2))/(coalesce(dep_cnt_3m,0)+dep_curr_cnt)*2.0 end as dep_curr_3v,
  case when dep_curr_cnt = 0 then 0 else cast(dep_curr_cnt as decimal(18,2))/(coalesce(dep_cnt_6m,0)+dep_curr_cnt)*2.0 end as dep_curr_6v,
  case when dep_curr_cnt = 0 then 0 else cast(dep_curr_cnt as decimal(18,2))/(coalesce(dep_cnt_12m,0)+dep_curr_cnt)*2.0 end as dep_curr_12v,
  case when dep_curr_amt = 0 then 0 else cast(dep_curr_amt as decimal(18,2))/(coalesce(dep_amt_3m,0)+dep_curr_amt)*2.0 end as dep_curr_amt_3v,
  case when dep_curr_amt = 0 then 0 else cast(dep_curr_amt as decimal(18,2))/(coalesce(dep_amt_6m,0)+dep_curr_amt)*2.0 end as dep_curr_amt_6v,
  case when dep_curr_amt = 0 then 0 else cast(dep_curr_amt as decimal(18,2))/(coalesce(dep_amt_12m,0)+dep_curr_amt)*2.0 end as dep_curr_amt_12v,
  dep_amt_3m,
  dep_amt_6m,
  dep_amt_12m,
  dep_max_amt_3m, 
  dep_max_amt_6m, 
  dep_max_amt_12m

from (SELECT  
        a.idclient,
        a.idblank,
        a.contact_dt,   
        count(distinct cas.idblank) as dep_hist_cnt,
        sum(case when (a.contact_dt between d_begin and d_end) and coalesce(close_date,sysdate) > a.contact_dt then 1 else 0 end) as dep_curr_cnt,
        sum(case when (a.contact_dt between d_begin and d_end) and coalesce(close_date,sysdate) > a.contact_dt then local_amount else 0 end) as dep_curr_amt,
        sum(case when (add_months(a.contact_dt,-3) between d_begin and d_end) and (open_date < add_months(a.contact_dt,-3)) and (coalesce(close_date,sysdate) > add_months(a.contact_dt,-3)) then 1 else 0 end) as dep_cnt_3m,
        sum(case when (add_months(a.contact_dt,-6) between d_begin and d_end) and (open_date < add_months(a.contact_dt,-6)) and (coalesce(close_date,sysdate) > add_months(a.contact_dt,-6)) then 1 else 0 end) as dep_cnt_6m,
        sum(case when (add_months(a.contact_dt,-12) between d_begin and d_end) and (open_date < add_months(a.contact_dt,-12)) and (coalesce(close_date,sysdate) > add_months(a.contact_dt,-12)) then 1 else 0 end) as dep_cnt_12m,
        sum(case when (add_months(a.contact_dt,-3) between d_begin and d_end) and (open_date < add_months(a.contact_dt,-3)) and (coalesce(close_date,sysdate) > add_months(a.contact_dt,-3)) then local_amount else 0 end) as dep_amt_3m,
        sum(case when (add_months(a.contact_dt,-6) between d_begin and d_end) and (open_date < add_months(a.contact_dt,-6)) and (coalesce(close_date,sysdate) > add_months(a.contact_dt,-6)) then local_amount else 0 end) as dep_amt_6m,
        sum(case when (add_months(a.contact_dt,-12) between d_begin and d_end) and (open_date < add_months(a.contact_dt,-12)) and (coalesce(close_date,sysdate) > add_months(a.contact_dt,-12)) then local_amount else 0 end) as dep_amt_12m,
        max(case when (d_end > add_months(a.contact_dt,-3)) and (d_begin < a.contact_dt)  then local_amount else 0 end) as dep_max_amt_3m, 
        max(case when (d_end > add_months(a.contact_dt,-6)) and (d_begin < a.contact_dt)  then local_amount else 0 end) as dep_max_amt_6m, 
        max(case when (d_end > add_months(a.contact_dt,-12)) and (d_begin < a.contact_dt)  then local_amount else 0 end) as dep_max_amt_12m
  
   from (SELECT 
          a.idclient,
          a.idblank,
          a.contact_dt
        from  SLV_CS_NEW_BASE a
        group by a.idclient, a.idblank, a.contact_dt
       ) a
        
  inner join CRM_USER.ES_DEPOSIT_REP_DATA_DAILY cas 
      on a.idclient = cas.idclient
      and cas.open_date < a.contact_dt
  group by a.idclient, a.idblank, a.contact_dt
  ) a
;

---------------------------------------------------------------------------------------------
--Балансы на КК
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S17
CREATE TABLE TMP_SLV_CS_NEW_S17 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  coalesce(cc_cur_bal,0) as cc_cur_bal,
  coalesce(cc_bal_3m,0) as cc_bal_3m,
  coalesce(cc_bal_6m,0) as cc_bal_6m,
  coalesce(cc_bal_12m,0) as cc_bal_12m,
  coalesce(cc_min_bal_3m,0) as cc_min_bal_3m, 
  coalesce(cc_min_bal_6m,0) as cc_min_bal_6m,
  coalesce(cc_min_bal_12m,0) as cc_min_bal_12m,
  coalesce(cc_avg_bal_3m,0) as cc_avg_bal_3m,
  coalesce(cc_avg_bal_6m,0) as cc_avg_bal_6m,
  coalesce(cc_avg_bal_12m,0) as cc_avg_bal_12m,
  (case when coalesce(cc_avg_bal_3m,0) = 0 then 0 else coalesce(cc_cur_bal,0)/cc_avg_bal_3m end) as cc_bal_3v,
  (case when coalesce(cc_avg_bal_6m,0) = 0 then 0 else coalesce(cc_cur_bal,0)/cc_avg_bal_6m end) as cc_bal_6v,
  (case when coalesce(cc_avg_bal_12m,0) = 0 then 0 else coalesce(cc_cur_bal,0)/cc_avg_bal_12m end) as cc_bal_12v
  
from (SELECT
        a.idclient,
        a.idblank,
        a.contact_dt,
        sum(case when a.contact_dt between d.date_from and d.date_to then d.bal_amt end) as cc_cur_bal,
        sum(case when add_months(a.contact_dt,-3) between d.date_from and d.date_to then d.bal_amt end) as cc_bal_3m,
        sum(case when add_months(a.contact_dt,-6) between d.date_from and d.date_to then d.bal_amt end) as cc_bal_6m,
        sum(case when add_months(a.contact_dt,-12) between d.date_from and d.date_to then d.bal_amt end) as cc_bal_12m,
        min(case when (d.date_to > add_months(a.contact_dt,-3)) and (d.date_from < a.contact_dt) then d.bal_amt end) as cc_min_bal_3m,
        min(case when (d.date_to > add_months(a.contact_dt,-6)) and (d.date_from < a.contact_dt) then d.bal_amt end) as cc_min_bal_6m,
        min(case when (d.date_to > add_months(a.contact_dt,-12)) and (d.date_from < a.contact_dt) then d.bal_amt end) as cc_min_bal_12m,
        avg(case when (d.date_to > add_months(a.contact_dt,-3)) and (d.date_from < a.contact_dt) then d.bal_amt end) as cc_avg_bal_3m,
        avg(case when (d.date_to > add_months(a.contact_dt,-6)) and (d.date_from < a.contact_dt) then d.bal_amt end) as cc_avg_bal_6m,
        avg(case when (d.date_to > add_months(a.contact_dt,-12)) and (d.date_from < a.contact_dt) then d.bal_amt end) as cc_avg_bal_12m

      from SLV_CS_NEW_BASE a

      inner join AGR_BLANKS b 
        on a.idclient = b.idclient
        and a.idblank != b.idblank
        and b.datestart < a.contact_dt
        and coalesce(b.datexit,sysdate) >= add_months(a.contact_dt,-12)

      inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
        on b.idcardtype = c.prod_card_type_id
        and c.prod_grp_id = 100000022
  
      inner join agr_bal_hist d
        on b.idblank = d.idblank
        and d.date_to >= add_months(a.contact_dt,-12)
      group by a.idclient, a.idblank, a.contact_dt
      ) a
;



---------------------------------------------------------------------------------------------
--Балансы на ДК
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S18
CREATE TABLE TMP_SLV_CS_NEW_S18 AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  coalesce(dc_cur_bal,0) as dc_cur_bal,
  coalesce(dc_bal_3m,0) as dc_bal_3m,
  coalesce(dc_bal_6m,0) as dc_bal_6m,
  coalesce(dc_bal_12m,0) as dc_bal_12m,
  coalesce(dc_max_bal_3m,0) as dc_max_bal_3m,
  coalesce(dc_max_bal_6m,0) as dc_max_bal_6m,
  coalesce(dc_max_bal_12m,0) as dc_max_bal_12m,
  coalesce(dc_avg_bal_3m,0) as dc_avg_bal_3m,
  coalesce(dc_avg_bal_6m,0) as dc_avg_bal_6m,
  coalesce(dc_avg_bal_12m,0) as dc_avg_bal_12m,
  (case when coalesce(dc_avg_bal_3m,0) = 0 then 0 else coalesce(dc_cur_bal,0)/dc_avg_bal_3m end) as dc_bal_3v,
  (case when coalesce(dc_avg_bal_6m,0) = 0 then 0 else coalesce(dc_cur_bal,0)/dc_avg_bal_6m end) as dc_bal_6v,
  (case when coalesce(dc_avg_bal_12m,0) = 0 then 0 else coalesce(dc_cur_bal,0)/dc_avg_bal_12m end) as dc_bal_12v
  

from (SELECT
        a.idclient,
        a.idblank,
        a.contact_dt,
        sum(case when a.contact_dt between d.date_from and d.date_to then d.bal_amt end) as dc_cur_bal,
        sum(case when add_months(a.contact_dt,-3) between d.date_from and d.date_to then d.bal_amt end) as dc_bal_3m,
        sum(case when add_months(a.contact_dt,-6) between d.date_from and d.date_to then d.bal_amt end) as dc_bal_6m,
        sum(case when add_months(a.contact_dt,-12) between d.date_from and d.date_to then d.bal_amt end) as dc_bal_12m,
        max(case when (d.date_to > add_months(a.contact_dt,-3)) and (d.date_from < a.contact_dt) then d.bal_amt end) as dc_max_bal_3m,
        max(case when (d.date_to > add_months(a.contact_dt,-6)) and (d.date_from < a.contact_dt) then d.bal_amt end) as dc_max_bal_6m,
        max(case when (d.date_to > add_months(a.contact_dt,-12)) and (d.date_from < a.contact_dt) then d.bal_amt end) as dc_max_bal_12m,
        avg(case when (d.date_to > add_months(a.contact_dt,-3)) and (d.date_from < a.contact_dt) then d.bal_amt end) as dc_avg_bal_3m,
        avg(case when (d.date_to > add_months(a.contact_dt,-6)) and (d.date_from < a.contact_dt) then d.bal_amt end) as dc_avg_bal_6m,
        avg(case when (d.date_to > add_months(a.contact_dt,-12)) and (d.date_from < a.contact_dt) then d.bal_amt end) as dc_avg_bal_12m

      from SLV_CS_NEW_BASE a

      inner join AGR_BLANKS b 
        on a.idclient = b.idclient
        and a.idblank != b.idblank
        and b.datestart < a.contact_dt
        and coalesce(b.datexit,sysdate) >= add_months(a.contact_dt,-12)

      inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
        on b.idcardtype = c.prod_card_type_id
        and c.prod_cat_id = 100000005
        and c.prod_grp_id != 100000022
  
      inner join agr_bal_hist d
        on b.idblank = d.idblank
        and d.date_to >= add_months(a.contact_dt,-12)
      group by a.idclient, a.idblank, a.contact_dt
      ) a
;
------------------------------------------------------------------------------------------
--Страховки дополнение
------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_S19
CREATE TABLE TMP_SLV_CS_NEW_S19 AS 
SELECT
    a.idclient,
    a.idblank,
    a.contact_dt, 
    sum(case when (b.datestart < add_months(a.contact_dt,-3)) and (coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-3)) then 1 else 0 end) as insure_qty_3,
    sum(case when  (b.datestart < add_months(a.contact_dt,-6)) and (coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-6)) then 1 else 0 end) as insure_qty_6,
    sum(case when (b.datestart < add_months(a.contact_dt,-12)) and (coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-12)) then 1 else 0 end) as insure_qty_12,
    sum(case when  (b.datestart < add_months(a.contact_dt,-24)) and (coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-24)) then 1 else 0 end) as insure_qty_24,
    sum(case when (b.datestart < add_months(a.contact_dt,-60)) and (coalesce(trunc(b.datexit), trunc(sysdate)) > add_months(a.contact_dt,-60)) then 1 else 0 end) as insure_qty_60

from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b
      on a.idclient = b.idclient
      and b.datestart < a.contact_dt

inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
      on b.idcardtype = c.prod_card_type_id
      and c.prod_cat_id in (100001655)
group by a.idclient, a.idblank, a.contact_dt
;
 
---------------------------
--mcc коды
---------------------------
--drop table TMP_SLV_CS_NEW_S8
--select * from TMP_SLV_CS_NEW_S20 where txn_amt = 0
CREATE TABLE TMP_SLV_CS_NEW_S20 AS
SELECT /*+ use_hash(a,b,c,d,e)*/
  a.idclient,
  a.idblank,
  a.contact_dt,
 -- c.prod_grp_id,
case  e.mcc_group when 'Housing'
          then to_number('1')
           when 'Organizations'
          then to_number('2')
            when 'Restaurants'
          then to_number('3')
            when 'Entertainment'
          then to_number('4')
            when 'General consumption'
          then to_number('5')
            when 'Gifts'
          then to_number('6')
            when 'Cash'
          then to_number('7')
            when 'Travel'
          then to_number('8')
            when 'Hobby'
          then to_number('9') 
            when 'Services'
          then to_number('10') 
            when 'Cars'
          then to_number('11')
            when 'Beauty'
          then to_number('12')
            when 'Electronics'
          then to_number('13')
            when 'Education'
          then to_number('14')
            when 'EServices'
          then to_number('15') 
            when 'Finance'
          then to_number('16')
            when 'Clothes'
          then to_number('17')           
            when 'Luxury'
          then to_number('18')
            when 'Sport'
          then to_number('19') 
            when 'Health'
          then to_number('20')
            when 'Transport'
          then to_number('21')                        
            else null end cat_txn_num,
  trunc(d.txn_fact_dt,'mm') as month_begin,
  sum(d.sum_amt_rur) as txn_amt
  
  from SLV_CS_NEW_BASE a
       
  inner join AGR_BLANKS b 
    on a.idclient = b.idclient
    and a.idblank != b.idblank
    and b.datestart < a.contact_dt

  inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
    on b.idcardtype = c.prod_card_type_id
    and c.prod_cat_id = 100000005

  inner join AGR_CARD_TRXN d
        on b.idblank = d.idblank
        and d.txn_fact_dt >= add_months(a.contact_dt,-12)
        and d.txn_fact_dt <  a.contact_dt
        and d.tran_group_code between 1 and 10-- расх тр-и
 
 inner join lib_mcc_code e
    on to_number(d.mcc_code) = e.mcc_code
 group by a.idclient, a.idblank, a.contact_dt,/* c.prod_grp_id, */trunc(d.txn_fact_dt,'mm'),
 case  e.mcc_group when 'Housing'
          then to_number('1')
           when 'Organizations'
          then to_number('2')
            when 'Restaurants'
          then to_number('3')
            when 'Entertainment'
          then to_number('4')
            when 'General consumption'
          then to_number('5')
            when 'Gifts'
          then to_number('6')
            when 'Cash'
          then to_number('7')
            when 'Travel'
          then to_number('8')
            when 'Hobby'
          then to_number('9') 
            when 'Services'
          then to_number('10') 
            when 'Cars'
          then to_number('11')
            when 'Beauty'
          then to_number('12')
            when 'Electronics'
          then to_number('13')
            when 'Education'
          then to_number('14')
            when 'EServices'
          then to_number('15') 
            when 'Finance'
          then to_number('16')
            when 'Clothes'
          then to_number('17')           
            when 'Luxury'
          then to_number('18')
            when 'Sport'
          then to_number('19') 
            when 'Health'
          then to_number('20')
            when 'Transport'
          then to_number('21')                        
            else null end 
 ;
 
 ----------------------
 
 CREATE TABLE TMP_SLV_CS_NEW_S23 AS
SELECT /*+ use_hash(a,b,c,d,e)*/
  a.idclient,
  a.idblank,
  a.contact_dt,
  count(distinct(case when d.txn_fact_dt >= add_months(a.contact_dt,-12) and d.txn_fact_dt < add_months(a.contact_dt,-6)  then e.mcc_group else null end)) as mcc_cnt_12_6,
  count(distinct(case when d.txn_fact_dt >= add_months(a.contact_dt,-6)  then e.mcc_group else null end)) as mcc_cnt_6,
  count(distinct(case when d.txn_fact_dt >= add_months(a.contact_dt,-3)  then e.mcc_group else null end)) as mcc_cnt_3
  from SLV_CS_NEW_BASE a
       
  inner join AGR_BLANKS b 
    on a.idclient = b.idclient
    and a.idblank != b.idblank
    and b.datestart < a.contact_dt

  inner join CRM_USER.LIB_PRODUCT_HIERARCHY c
    on b.idcardtype = c.prod_card_type_id
    and c.prod_cat_id = 100000005

  inner join AGR_CARD_TRXN d
        on b.idblank = d.idblank
        and d.txn_fact_dt >= add_months(a.contact_dt,-12)
        and d.txn_fact_dt <  a.contact_dt
        and d.tran_group_code between 1 and 10-- расх тр-и
 
 inner join lib_mcc_code e
    on to_number(d.mcc_code) = e.mcc_code
 group by a.idclient, a.idblank, a.contact_dt
 ;
------------------------------------
--------mcc агрегат
------------------------------------
CREATE TABLE TMP_SLV_CS_NEW_S21 AS
select r.idclient,
r.idblank,
r.contact_dt,
q.max_cat_mcc_12m,
e.max_cat_mcc_12_6m,
w.max_cat_mcc_6m
 from SLV_CS_NEW_BASE r join
(select idclient,
idblank,
contact_dt,
cat_txn_num as max_cat_mcc_12m
from
        (select idclient,idblank,cat_txn_num,contact_dt,row_number() over (partition by idclient, idblank, contact_dt order by txn_mcc_12m desc) as rn 
        from
        (select  a.idclient,
                 a.idblank,
                 a.contact_dt,
                 a.cat_txn_num,
                 nvl(sum(a.txn_amt),0) as txn_mcc_12m
           from TMP_SLV_CS_NEW_S20 a 
           group by   a.idclient, a.idblank, a.contact_dt, a.cat_txn_num)

) b where rn = 1) q on
   r.idclient = q.idclient
  and r.idblank = q.idblank
  and r.contact_dt = q.contact_dt join

(select idclient,
idblank,
contact_dt,
cat_txn_num as max_cat_mcc_6m
from
        (select idclient,idblank,cat_txn_num,contact_dt,row_number() over (partition by idclient, idblank, contact_dt order by txn_mcc_6m desc) as rn 
        from
        (select  a.idclient,
                 a.idblank,
                 a.contact_dt,
                 a.cat_txn_num,
                 nvl(sum(case when a.month_begin >= add_months(a.contact_dt,-6) then txn_amt else 0 end),0) as txn_mcc_6m
           from TMP_SLV_CS_NEW_S20 a 
           group by   a.idclient, a.idblank, a.contact_dt, a.cat_txn_num)

) b where rn = 1
)w
  on r.idclient = w.idclient
  and r.idblank = w.idblank
  and r.contact_dt = w.contact_dt join

(select idclient,
idblank,
contact_dt,
cat_txn_num as max_cat_mcc_12_6m
from
        (select idclient,idblank,cat_txn_num,contact_dt,row_number() over (partition by idclient, idblank, contact_dt order by txn_mcc_12_6m desc) as rn 
        from
        (select  a.idclient,
                 a.idblank,
                 a.contact_dt,
                 a.cat_txn_num,
                 sum(case when a.month_begin >= add_months(a.contact_dt,-12) and a.month_begin < add_months(a.contact_dt,-6)  then txn_amt else 0 end) as txn_mcc_12_6m
           from TMP_SLV_CS_NEW_S20 a 
           group by   a.idclient, a.idblank, a.contact_dt, a.cat_txn_num)

) b where rn = 1)e
  on r.idclient = e.idclient
  and r.idblank = e.idblank
  and r.contact_dt = e.contact_dt

------------------------------------
--Типы карт
------------------------------------

CREATE TABLE TMP_SLV_CS_NEW_S24 AS
select
  a.idclient,
  a.idblank,
  a.contact_dt,
  max(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' then 1 else 0 end) as gold_card_hist_flg,
  max(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' 
             or upper(c.CARD_PLASTIC_GRP_NAME) like '%CLASSIC%' then 1 else 0 end) as cls_std_card_hist_flg,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' then 1 else 0 end) as gold_card_hist_cnt,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' 
             or upper(c.CARD_PLASTIC_GRP_NAME) like '%CLASSIC%' then 1 else 0 end) as cls_std_card_hist_cnt,
  max(case when c.PROD_LEVEL_TYPE_NAME = 'Upper Mass' then 1 else 0 end) as up_mass_card_flg,
  max(case when c.PROD_LEVEL_TYPE_NAME = 'Mass' then 1 else 0 end) as mass_card_flg,
  max(case when c.PROD_LEVEL_TYPE_NAME = 'Exclusive' then 1 else 0 end) as excl_card_flg,
  max(case when c.PROD_LEVEL_TYPE_NAME = 'Middle' then 1 else 0 end) as mid_card_flg,
  sum(case when c.PROD_LEVEL_TYPE_NAME = 'Upper Mass' then 1 else 0 end) as up_mass_card_cnt,
  sum(case when c.PROD_LEVEL_TYPE_NAME = 'Mass' then 1 else 0 end) as mass_card_cnt,
  sum(case when c.PROD_LEVEL_TYPE_NAME = 'Exclusive' then 1 else 0 end) as excl_card_cnt,
  sum(case when c.PROD_LEVEL_TYPE_NAME = 'Middle' then 1 else 0 end) as mid_card_cnt

from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b
  on a.idclient  = b.idclient
  and a.idblank != b.idblank
  and b.datestart < a.contact_dt

inner join LIB_PRODUCT_HIERARCHY c
  on b.idcardtype = c.prod_card_type_id
  and c.prod_cat_id = 100000005
group by a.idclient, a.idblank, a.contact_dt;



CREATE TABLE TMP_SLV_CS_NEW_S25 AS
select
  a.idclient,
  a.idblank,
  a.contact_dt,
  case when a.PROD_LEVEL_TYPE_NAME = 'Upper Mass' then 1 else 0 end as last_up_mass_flg, 
  case when a.PROD_LEVEL_TYPE_NAME = 'Mass' then 1 else 0 end as last_mass_flg, 
  case when a.PROD_LEVEL_TYPE_NAME = 'Middle' then 1 else 0 end as last_middle_flg
from 
    (select
      a.idclient,
      a.idblank,
      a.contact_dt,
      a.PROD_LEVEL_TYPE_NAME
    from
        (select
          a.idclient,
          a.idblank,
          a.contact_dt,
          c.PROD_LEVEL_TYPE_NAME,
          row_number() over (partition by a.idclient, a.idblank, a.contact_dt order by b.datestart desc) as rn 

        from SLV_CS_NEW_BASE a

        inner join AGR_BLANKS b
          on a.idclient  = b.idclient
          and a.idblank != b.idblank
          and b.datestart < a.contact_dt

        inner join LIB_PRODUCT_HIERARCHY c
          on b.idcardtype = c.prod_card_type_id
          and c.prod_cat_id = 100000005
        ) a
    where rn = 1) a
;

CREATE TABLE TMP_SLV_CS_NEW_S26 AS
select
  a.idclient,
  a.idblank,
  a.contact_dt,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' and d.txn_fact_dt >= add_months(a.contact_dt,-3) then d.sum_amt_rur else 0 end) as gold_txn_amt_3m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' and d.txn_fact_dt >= add_months(a.contact_dt,-6) then d.sum_amt_rur else 0 end) as gold_txn_amt_6m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' and d.txn_fact_dt >= add_months(a.contact_dt,-12) then d.sum_amt_rur else 0 end) as gold_txn_amt_12m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' and d.txn_fact_dt >= add_months(a.contact_dt,-24) then d.sum_amt_rur else 0 end) as gold_txn_amt_24m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' and d.txn_fact_dt >= add_months(a.contact_dt,-36) then d.sum_amt_rur else 0 end) as gold_txn_amt_36m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' and d.txn_fact_dt >= add_months(a.contact_dt,-3) then d.sum_amt_rur else 0 end) as std_txn_amt_3m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' and d.txn_fact_dt >= add_months(a.contact_dt,-6) then d.sum_amt_rur else 0 end) as std_txn_amt_6m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' and d.txn_fact_dt >= add_months(a.contact_dt,-12) then d.sum_amt_rur else 0 end) as std_txn_amt_12m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' and d.txn_fact_dt >= add_months(a.contact_dt,-24) then d.sum_amt_rur else 0 end) as std_txn_amt_24m,
  sum(case when upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%' and d.txn_fact_dt >= add_months(a.contact_dt,-36) then d.sum_amt_rur else 0 end) as std_txn_amt_36m


from SLV_CS_NEW_BASE a

inner join AGR_BLANKS b
  on a.idclient  = b.idclient
  and a.idblank != b.idblank
  and b.datestart < a.contact_dt

inner join LIB_PRODUCT_HIERARCHY c
  on b.idcardtype = c.prod_card_type_id
  and c.prod_cat_id = 100000005
  and (upper(c.CARD_PLASTIC_GRP_NAME) like '%GOLD%' or upper(c.CARD_PLASTIC_GRP_NAME) like '%STANDARD%')

inner join AGR_CARD_TRXN d
  on b.idblank = d.idblank
  and d.txn_fact_dt >= add_months(a.contact_dt,-36)
group by a.idclient, a.idblank, a.contact_dt;  

--------------------------------------
--Транзакционные интервалы
-------------------------------------
--drop table TMP_SLV_INT_TXN     
CREATE TABLE TMP_SLV_TXN AS
select 
  a.idclient,
  a.idblank,
  a.contact_dt,
  d.txn_fact_dt,
  row_number() over (partition by a.idclient, a.idblank, a.contact_dt order by d.txn_fact_dt desc) as rn,
  max(d.txn_fact_dt) over (partition by a.idclient, a.idblank, a.contact_dt 
                           order by d.txn_fact_dt desc rows between 1 following and 1 following) as prev_trx_dt

              from SLV_CS_NEW_BASE a

              inner join AGR_CARD_TRXN d
                 on a.idclient = d.idclient
                 and a.idblank != d.idblank
                 and d.txn_fact_dt between add_months(a.contact_dt,-6) and (a.contact_dt)
                 and d.tran_group_code between 1 and 10

     --drop table TMP_SLV_CS_NEW_S27
CREATE TABLE TMP_SLV_CS_NEW_S27 AS
 select
  b.idclient,
  b.idblank,
  b.contact_dt,
   min(b.days_between_trxns_6) as btw_txn_min_6m,
   max(b.days_between_trxns_6) as btw_txn_max_6m,
   avg(b.days_between_trxns_6)  as btw_txn_avr_6m,
   min(b.days_between_trxns_1) as btw_txn_min_1m,
   max(b.days_between_trxns_1) as btw_txn_max_1m,
   avg(b.days_between_trxns_1)  as btw_txn_avr_1m,
   min(b.days_between_trxns_3) as btw_txn_min_3m,
   max(b.days_between_trxns_3) as btw_txn_max_3m,
   avg(b.days_between_trxns_3)  as btw_txn_avr_3m,
   variance(b.days_between_trxns_6)as btw_txn_var_6m
   from 
            (select idclient,
                  idblank,
                  contact_dt,
                  (case when txn_fact_dt>=add_months(contact_dt,-1) and prev_trx_dt>=add_months(contact_dt,-1)then(txn_fact_dt - prev_trx_dt)  end)as days_between_trxns_1,
                  (case when txn_fact_dt>=add_months(contact_dt,-3) and prev_trx_dt>=add_months(contact_dt,-3)then(txn_fact_dt - prev_trx_dt)  end) as days_between_trxns_3,
                  (case when txn_fact_dt>=add_months(contact_dt,-6) and prev_trx_dt>=add_months(contact_dt,-6)then (txn_fact_dt - prev_trx_dt)  end) as days_between_trxns_6
             from TMP_SLV_TXN) b
  group by b.idclient,
  b.idblank,
  b.contact_dt;
select * from TMP_SLV_CS_NEW_S27


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Добавляем данные об использовании ИБ и МБ
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Посещение ИБ
--drop table TMP_SLV_CS_NEW_S29
CREATE TABLE TMP_SLV_CS_NEW_S29 AS
select  /* +use_hash(a,b,c,d,e) */ 
a.idclient,
a.idblank,
a.contact_dt,
count (user_session_id) as cnt_ib_6m,
sum(case when b.us_begin_time > add_months(a.contact_dt,-3) then 1 else 0 end) as cnt_ib_3m,
sum(case when b.us_begin_time > add_months(a.contact_dt,-1) then 1 else 0 end) as cnt_ib_1m
              from SLV_CS_NEW_BASE a
                     inner join orawh.IB_CLIENT_EXT_MV e
                         on a.idclient = e.client_id
                     inner join orawh.IB_CLIENT_MV d
                       on e.client_id  = d.client_id 
                     inner join orawh.IB_USERS_MV c
                       on d.PERSON_ID  = c.PERSON_ID 
                     inner join orawh.ib_user_session_mv b
                       on b.user_id = c.user_id
--and b.US_END_TIME  between add_months(a.contact_dt,-6) and (a.contact_dt)
                      and b.us_begin_time between add_months(a.contact_dt,-6) and (a.contact_dt)
group by a.idclient,
a.idblank,
a.contact_dt
--MB
--drop table TMP_SLV_CS_NEW_S28
select * from TMP_SLV_CS_NEW_S28
CREATE TABLE TMP_SLV_CS_NEW_S28 AS
select  /* +use_hash(a,b,c,d,e) */ 
a.idclient,
a.idblank,
a.contact_dt,
count (IND) as cnt_mb_6m,
sum(case when ml.OP_DATE > add_months(a.contact_dt,-3) then 1 else 0 end) as cnt_mb_3m,
sum(case when ml.OP_DATE > add_months(a.contact_dt,-1) then 1 else 0 end) as cnt_mb_1m
              from SLV_CS_NEW_BASE a
                     inner join orawh.MB_OPERATION_LIST ml
                     on a.idclient = ml.idclient
                     and ml.OP_DATE between add_months(a.contact_dt,-6) and (a.contact_dt)
                     and ml.category_id = 900
group by a.idclient,
a.idblank,
a.contact_dt
---------------------------------------------------------------------------------------------
--Сборка
---------------------------------------------------------------------------------------------
--drop table TMP_SLV_CS_NEW_V
CREATE TABLE TMP_SLV_CS_NEW_V AS
SELECT
  a.idclient,
  a.idblank,
  a.contact_dt,
  a.event,
  a.segm,
  coalesce(b.pp_hist_flg,0) as pp_hist_flg,
  coalesce(b.pp_curr_flg,0) as pp_curr_flg,
  coalesce(b.pos_hist_flg,0) as pos_hist_flg,
  coalesce(b.pil_hist_flg,0) as pil_hist_flg,
  coalesce(b.pos_curr_flg,0) as pos_curr_flg,
  coalesce(b.pil_curr_flg,0) as pil_curr_flg,
  coalesce(b.pp_hist_cnt,0) as pp_hist_cnt,
  coalesce(b.pp_curr_cnt,0) as pp_curr_cnt,
  coalesce(b.pos_hist_cnt,0) as pos_hist_cnt,
  coalesce(b.pil_hist_cnt,0) as pil_hist_cnt,
  coalesce(b.pos_curr_cnt,0) as pos_curr_cnt,
  coalesce(b.pil_curr_cnt,0) as pil_curr_cnt,
  case when b.mnth_from_first_pos is not null then (cast((b.mnth_from_first_pos) as integer)) else -1 end as  mnth_from_first_pos,
  case when b.mnth_from_last_pos is not null then (cast((b.mnth_from_last_pos) as integer)) else -1 end as  mnth_from_last_pos,
  case when b.mnth_from_first_pil is not null then (cast((b.mnth_from_first_pil) as integer)) else -1 end as  mnth_from_first_pil,
  case when b.mnth_from_last_pil is not null then (cast((b.mnth_from_last_pil) as integer)) else -1 end as  mnth_from_last_pil,
  case when b.mnth_from_first_pp is not null then (cast((b.mnth_from_first_pp) as integer)) else -1 end as  mnth_from_first_pp,
  case when b.mnth_from_last_pp is not null then (cast((b.mnth_from_last_pp) as integer)) else -1 end as  mnth_from_last_pp,
  
  coalesce(c.card_hist_flg,0) as card_hist_flg,
  coalesce(c.card_curr_flg,0) as card_curr_flg,
  coalesce(c.cc_hist_flg,0) as cc_hist_flg,
  coalesce(c.dc_hist_flg,0) as dc_hist_flg,
  coalesce(c.cc_curr_flg,0) as cc_curr_flg,
  coalesce(c.dc_curr_flg,0) as dc_curr_flg,
  coalesce(c.card_hist_cnt,0) as card_hist_cnt,
  coalesce(c.card_curr_cnt,0) as card_curr_cnt,
  coalesce(c.cc_hist_cnt,0) as cc_hist_cnt,
  coalesce(c.dc_hist_cnt,0) as dc_hist_cnt,
  coalesce(c.cc_curr_cnt,0) as cc_curr_cnt,
  coalesce(c.dc_curr_cnt,0) as dc_curr_cnt,
  case when c.mnth_from_first_cc is not null then (cast((c.mnth_from_first_cc) as integer)) else -1 end as mnth_from_first_cc,
  case when c.mnth_from_last_cc is not null then (cast((c.mnth_from_last_cc) as integer)) else -1 end as mnth_from_last_cc,
  case when c.mnth_from_first_dc is not null then (cast((c.mnth_from_first_dc) as integer)) else -1 end as mnth_from_first_dc,
  case when c.mnth_from_last_dc is not null then (cast((c.mnth_from_last_dc) as integer)) else -1 end as mnth_from_last_dc,
  case when c.mnth_from_first_card is not null then (cast((c.mnth_from_first_card) as integer)) else -1 end as mnth_from_first_card,
  case when c.mnth_from_last_card is not null then (cast((c.mnth_from_last_card) as integer)) else -1 end as mnth_from_last_card,
  
  coalesce(d.restr_hist_flg,0) as restr_hist_flg,
  coalesce(d.restr_curr_flg,0) as restr_curr_flg,
  coalesce(d.insur_hist_flg,0) as insur_hist_flg,
  coalesce(d.insur_curr_flg,0) as insur_curr_flg,
  coalesce(d.restr_hist_cnt,0) as restr_hist_cnt,
  coalesce(d.restr_curr_cnt,0) as restr_curr_cnt,
  coalesce(d.insur_hist_cnt,0) as insur_hist_cnt,
  coalesce(d.insur_curr_cnt,0) as insur_curr_cnt,
  case when d.mnth_from_first_restr is not null then (cast((d.mnth_from_first_restr) as integer)) else -1 end as mnth_from_first_restr,
  case when d.mnth_from_last_restr is not null then (cast((d.mnth_from_last_restr) as integer)) else -1 end as mnth_from_last_restr,
  case when d.mnth_from_first_insur is not null then (cast((d.mnth_from_first_insur) as integer)) else -1 end as mnth_from_first_insur,
  case when d.mnth_from_last_insur is not null then (cast((d.mnth_from_last_insur) as integer)) else -1 end as mnth_from_last_insur,
  
  coalesce(f.cnt_act_cc_3v,0) as cnt_act_cc_3v,
  coalesce(f.cnt_act_cc_6v,0) as cnt_act_cc_6v,
  coalesce(f.cnt_act_cc_12v,0) as cnt_act_cc_12v,
  
  coalesce(g.cnt_act_dc_3v,0) as cnt_act_dc_3v,
  coalesce(g.cnt_act_dc_6v,0) as cnt_act_dc_6v,
  coalesce(g.cnt_act_dc_12v,0) as cnt_act_dc_12v,
  
  coalesce(h.cnt_act_pos_3v,0) as cnt_act_pos_3v,
  coalesce(h.cnt_act_pos_6v,0) as cnt_act_pos_6v,
  coalesce(h.cnt_act_pos_12v,0) as cnt_act_pos_12v,
  
  coalesce(j.cnt_arc_prod,0) as cnt_arc_prod,
  coalesce(j.cnt_arc_prod_v3,0) as cnt_arc_prod_v3,
  coalesce(j.cnt_arc_prod_v6,0) as cnt_arc_prod_v6,
  coalesce(j.cnt_arc_prod_v12,0) as cnt_arc_prod_v12,
  
  k.cl_txn_type,
  
  coalesce(l.cc_pos_amt_3m,0) as cc_pos_amt_3m,
  coalesce(l.cc_pos_amt_3_6m,0) as cc_pos_amt_3_6m,
  coalesce(l.cc_pos_amt_6_12m,0) as cc_pos_amt_6_12m,
  coalesce(l.cc_txn_amt_3m,0) as cc_txn_amt_3m,
  coalesce(l.cc_txn_amt_3_6m,0) as cc_txn_amt_3_6m,
  coalesce(l.cc_txn_amt_6_12m,0) as cc_txn_amt_6_12m,
  coalesce(l.dc_pos_amt_3m,0) as dc_pos_amt_3m,
  coalesce(l.dc_pos_amt_3_6m,0) as dc_pos_amt_3_6m,
  coalesce(l.dc_pos_amt_6_12m,0) as dc_pos_amt_6_12m,
  coalesce(l.dc_txn_amt_3m,0) as dc_txn_amt_3m,
  coalesce(l.dc_txn_amt_3_6m,0) as dc_txn_amt_3_6m,
  coalesce(l.dc_txn_amt_6_12m,0) as dc_txn_amt_6_12m,
  coalesce(l.foreign_txn_0_6m,0) as foreign_txn_0_6m,
  coalesce(l.foreign_txn_6_12m,0) as foreign_txn_6_12m,
  coalesce(l.foreign_txn_12_18m,0) as foreign_txn_12_18m,
  coalesce(l.cc_cash_rsb_0_6m,0) as cc_cash_rsb_0_6m,
  coalesce(l.cc_cash_rsb_6_12m,0) as cc_cash_rsb_6_12m,
  coalesce(l.cc_cash_rsb_12_18m,0) as cc_cash_rsb_12_18m,
  coalesce(l.dc_cash_rsb_0_6m,0) as dc_cash_rsb_0_6m,
  coalesce(l.dc_cash_rsb_6_12m,0) as dc_cash_rsb_6_12m,
  coalesce(l.dc_cash_rsb_12_18m,0) as dc_cash_rsb_12_18m,
  coalesce(l.cash_no_rsb_0_6m,0) as cash_no_rsb_0_6m,
  coalesce(l.cash_no_rsb_6_12m,0) as cash_no_rsb_6_12m,
  coalesce(l.cash_no_rsb_12_18m,0) as cash_no_rsb_12_18m, 
  coalesce(l.no_txn_12,0) as no_txn_12,
  
  coalesce(m.inc_amt_3m,0) as inc_amt_3m,
  coalesce(m.inc_amt_3_6m,0) as inc_amt_3_6m,
  coalesce(m.inc_amt_6_12m,0) as inc_amt_6_12m,
  case when m.mnth_from_first_inc_txn is not null then (cast((m.mnth_from_first_inc_txn) as integer)) else -1 end as mnth_from_first_inc_txn,
  case when m.mnth_from_last_inc_txn is not null then (cast((m.mnth_from_last_inc_txn) as integer)) else -1 end as mnth_from_last_inc_txn,
  case when m.mnth_from_first_cash_txn is not null then (cast((m.mnth_from_first_cash_txn) as integer)) else -1 end as mnth_from_first_cash_txn,
  case when m.mnth_from_last_cash_txn is not null then (cast((m.mnth_from_last_cash_txn) as integer)) else -1 end as mnth_from_last_cash_txn,
  case when m.mnth_from_first_pos_txn is not null then (cast((m.mnth_from_first_pos_txn) as integer)) else -1 end as mnth_from_first_pos_txn,
  case when m.mnth_from_last_pos_txn is not null then (cast((m.mnth_from_last_pos_txn) as integer)) else -1 end as mnth_from_last_pos_txn,
  case when m.mnth_from_first_txn is not null then (cast((m.mnth_from_first_txn) as integer)) else -1 end as mnth_from_first_txn,
  case when m.mnth_from_last_txn is not null then (cast((m.mnth_from_last_txn) as integer)) else -1 end as mnth_from_last_txn,
  
  case when n.mnth_from_first_prod is not null then (cast((n.mnth_from_first_prod) as integer)) else -1 end as mnth_from_first_prod,
  case when n.mnth_from_last_prod is not null then (cast((n.mnth_from_last_prod) as integer)) else -1 end as mnth_from_last_prod,
  case when n.mnth_from_first_cred is not null then (cast((n.mnth_from_first_cred) as integer)) else -1 end as mnth_from_first_cred,
  case when n.mnth_from_last_cred is not null then (cast((n.mnth_from_last_cred) as integer)) else -1 end as mnth_from_last_cred,
  
  case when p.mnth_from_cred_entry is not null then (cast((p.mnth_from_cred_entry) as integer)) else -1 end as mnth_from_cred_entry,
  case when p.mnth_from_last_entry is not null then (cast((p.mnth_from_last_entry) as integer)) else -1 end as mnth_from_last_entry,
  case when p.mnth_from_fail_entry is not null then (cast((p.mnth_from_fail_entry) as integer))else -1 end as mnth_from_fail_entry,                                       
  coalesce(p.cnt_entry_fail_6m,0) as cnt_entry_fail_6m,
  coalesce(p.cnt_entry_6m,0) as cnt_entry_6m,
  coalesce(p.cnt_cc_entry_appr,0) as cnt_cc_entry_appr,
  
  q.age,
  coalesce(q.education,0) as edu_cd,
  coalesce(q.work_otrasle,0) as industry_cd,
  coalesce(q.work_doljn,0) as job_cd,
  coalesce(q.distance,0) as distance,
  coalesce(q.income,0) as income,
  coalesce(q.pol,0)as gender,
  coalesce(q.married,0) as married,
  coalesce(q.naxlebnik,0) as dependant,
  coalesce(q.dr_licence,0) as dr_licence,
  coalesce(q.children,0) as children,
  
  coalesce(r.cs_cnt,0) as cs_cnt,
  coalesce(r.cs_tkn_flg,0) as cs_tkn_flg,
  coalesce(r.cs_tkn_cnt,0) as cs_tkn_cnt,
  coalesce(r.cs_curr_cnt,0) as cs_curr_cnt,
  case when r.mnth_from_first_cs is not null then (cast((r.mnth_from_first_cs) as integer)) else -1 end as mnth_from_first_cs,                                       
  case when r.mnth_from_last_cs is not null then (cast((r.mnth_from_last_cs) as integer)) else -1 end as mnth_from_last_cs,
  
  coalesce(t.cc_cur_bal,0) as cc_cur_bal,
  coalesce(t.cc_bal_3m,0) as cc_bal_3m,
  coalesce(t.cc_bal_6m,0) as cc_bal_6m,
  coalesce(t.cc_bal_12m,0) as cc_bal_12m,
  coalesce(t.cc_min_bal_3m,0) as cc_min_bal_3m, 
  coalesce(t.cc_min_bal_6m,0) as cc_min_bal_6m,
  coalesce(t.cc_min_bal_12m,0) as cc_min_bal_12m,
  coalesce(t.cc_avg_bal_3m,0) as cc_avg_bal_3m,
  coalesce(t.cc_avg_bal_6m,0) as cc_avg_bal_6m,
  coalesce(t.cc_avg_bal_12m,0) as cc_avg_bal_12m,
  coalesce(t.cc_bal_3v,0) as cc_bal_3v,
  coalesce(t.cc_bal_6v,0) as cc_bal_6v,
  coalesce(t.cc_bal_12v,0) as cc_bal_12v,
  
  coalesce(v.dc_cur_bal,0) as dc_cur_bal,
  coalesce(v.dc_bal_3m,0) as dc_bal_3m,
  coalesce(v.dc_bal_6m,0) as dc_bal_6m,
  coalesce(v.dc_bal_12m,0) as dc_bal_12m,
  coalesce(v.dc_max_bal_3m,0) as dc_max_bal_3m,
  coalesce(v.dc_max_bal_6m,0) as dc_max_bal_6m,
  coalesce(v.dc_max_bal_12m,0) as dc_max_bal_12m,
  coalesce(v.dc_avg_bal_3m,0) as dc_avg_bal_3m,
  coalesce(v.dc_avg_bal_6m,0) as dc_avg_bal_6m,
  coalesce(v.dc_avg_bal_12m,0) as dc_avg_bal_12m,
  coalesce(v.dc_bal_3v,0) as dc_bal_3v,
  coalesce(v.dc_bal_6v,0) as dc_bal_6v,
  coalesce(v.dc_bal_12v,0) as dc_bal_12v,  
  
  coalesce(x.dep_hist_cnt,0) as dep_hist_cnt,
  coalesce(x.dep_curr_cnt,0) as dep_curr_cnt,
  coalesce(x.dep_curr_amt,0) as dep_curr_amt,
  coalesce(x.dep_curr_3v,0) as dep_curr_3v,
  coalesce(x.dep_curr_6v,0) as dep_curr_6v,
  coalesce(x.dep_curr_12v,0) as dep_curr_12v,
  coalesce(x.dep_curr_amt_3v,0) as dep_curr_amt_3v,
  coalesce(x.dep_curr_amt_6v,0) as dep_curr_amt_6v,
  coalesce(x.dep_curr_amt_12v,0) as dep_curr_amt_12v,
  coalesce(x.dep_amt_3m,0) as dep_amt_3m,
  coalesce(x.dep_amt_6m,0) as dep_amt_6m,
  coalesce(x.dep_amt_12m,0) as dep_amt_12m,
  coalesce(x.dep_max_amt_3m,0) as dep_max_amt_3m,
  coalesce(x.dep_max_amt_6m,0) as dep_max_amt_6m,
  coalesce(x.dep_max_amt_12m,0) as dep_max_amt_12m,

coalesce(a1.btw_txn_min_3m,-1) as  btw_txn_min_3m,
coalesce(a1.btw_txn_min_6m,-1) as  btw_txn_min_6m,
coalesce(a1.btw_txn_max_3m,-1) as btw_txn_max_3m,
coalesce(a1.btw_txn_max_6m,-1) as btw_txn_max_6m,
coalesce(a1.btw_txn_avr_3m,-1) as btw_txn_avr_3m,
coalesce(a1.btw_txn_avr_1m,-1)as btw_txn_avr_1m,
coalesce(a1.btw_txn_avr_6m,-1) as btw_txn_avr_6m,
coalesce(a1.btw_txn_var_6m,-1) as btw_txn_var_6m,

coalesce(a2.cnt_ib_3m,0) as cnt_ib_3m,  
coalesce(a2.cnt_ib_6m,0) as cnt_ib_6m,  
coalesce(a2.cnt_ib_1m,0) as cnt_ib_1m,  
coalesce(a3.cnt_mb_3m,0) as cnt_mb_3m,  
coalesce(a3.cnt_mb_6m,0) as cnt_mb_6m,  
coalesce(a3.cnt_mb_1m,0) as cnt_mb_1m

from SLV_CS_NEW_BASE a

left join TMP_SLV_CS_NEW_S1 b
  on a.idclient = b.idclient
  and a.idblank = b.idblank
  and a.contact_dt = b.contact_dt
  
left join TMP_SLV_CS_NEW_S2 c
  on a.idclient = c.idclient
  and a.idblank = c.idblank
  and a.contact_dt = c.contact_dt
  
left join TMP_SLV_CS_NEW_S3 d
  on a.idclient = d.idclient
  and a.idblank = d.idblank
  and a.contact_dt = d.contact_dt
  
left join TMP_SLV_CS_NEW_S4 f
  on a.idclient = f.idclient
  and a.idblank = f.idblank
  and a.contact_dt = f.contact_dt
  
left join TMP_SLV_CS_NEW_S5 g
  on a.idclient = g.idclient
  and a.idblank = g.idblank
  and a.contact_dt = g.contact_dt
  
left join TMP_SLV_CS_NEW_S6 h
  on a.idclient = h.idclient
  and a.idblank = h.idblank
  and a.contact_dt = h.contact_dt
  
left join TMP_SLV_CS_NEW_S7 j
  on a.idclient = j.idclient
  and a.idblank = j.idblank
  and a.contact_dt = j.contact_dt
  
left join TMP_SLV_CS_NEW_S9 k
  on a.idclient = k.idclient
  and a.idblank = k.idblank
  and a.contact_dt = k.contact_dt
  
left join TMP_SLV_CS_NEW_S10 l
  on a.idclient = l.idclient
  and a.idblank = l.idblank
  and a.contact_dt = l.contact_dt
  
left join TMP_SLV_CS_NEW_S11 m
  on a.idclient = m.idclient
  and a.idblank = m.idblank
  and a.contact_dt = m.contact_dt
  
left join TMP_SLV_CS_NEW_S12 n
  on a.idclient = n.idclient
  and a.idblank = n.idblank
  and a.contact_dt = n.contact_dt
  
left join TMP_SLV_CS_NEW_S13 p
  on a.idclient = p.idclient
  and a.idblank = p.idblank
  and a.contact_dt = p.contact_dt

left join TMP_SLV_CS_NEW_S14 q
  on a.idclient = q.idclient
  
left join TMP_SLV_CS_NEW_S15 r
  on a.idclient = r.idclient
  and a.idblank = r.idblank
  and a.contact_dt = r.contact_dt
  
left join CRM_USER.TMP_SLV_CS_S16 x 
  on a.idclient = x.idclient
  and a.idblank = x.idblank
  and a.contact_dt = x.contact_dt

left join TMP_SLV_CS_NEW_S17 t
  on a.idclient = t.idclient
  and a.idblank = t.idblank
  and a.contact_dt = t.contact_dt

left join TMP_SLV_CS_NEW_S18 v
  on a.idclient = v.idclient
  and a.idblank = v.idblank
  and a.contact_dt = v.contact_dt
  
left join TMP_SLV_CS_NEW_S27 a1
  on a.idclient = a1.idclient
  and a.idblank = a1.idblank
  and a.contact_dt = a1.contact_dt
  
left join TMP_SLV_CS_NEW_S28 a3
  on a.idclient = a3.idclient
  and a.idblank = a3.idblank
  and a.contact_dt = a3.contact_dt
  
left join TMP_SLV_CS_NEW_S29 a2
  on a.idclient = a2.idclient
  and a.idblank = a2.idblank
  and a.contact_dt = a2.contact_dt
;
 
--drop table TMP_SLV_CS_NEW_VV
--CREATE TABLE TMP_SLV_CS_NEW_VV AS
--select distinct * from TMP_SLV_CS_NEW_V;

CREATE TABLE SLV_CS_NEW_VV_NT AS
select * from TMP_SLV_CS_NEW_V;

CREATE TABLE SLV_CS_NEW_VV_NT_V2 AS
select 
 a.*,
  coalesce(b.insure_qty_3,0) as insure_qty_3,
  coalesce(b.insure_qty_6,0) as insure_qty_6,
  coalesce(b.insure_qty_12,0) as insure_qty_12,
  coalesce(b.insure_qty_24,0) as insure_qty_24,
  coalesce(b.insure_qty_60,0) as insure_qty_60,
  coalesce(c.max_cat_mcc_12m,0) as max_cat_mcc_12m,
  coalesce(c.max_cat_mcc_12_6m,0) as max_cat_mcc_12_6m,
  coalesce(c.max_cat_mcc_6m,0) as max_cat_mcc_6m,
  coalesce(d.txn_amt_1yr,0) as txn_amt_1yr,
  coalesce(d.txn_amt_2yr,0) as txn_amt_2yr,
  coalesce(d.txn_amt_3yr,0) as txn_amt_3yr,
  coalesce(d.rel_txn_1_2,0) as rel_txn_1_2,
  coalesce(d.rel_txn_1_3,0) as rel_txn_1_3,
  coalesce(d.rel_txn_2_3,0) as rel_txn_2_3,
  coalesce(e.mcc_cnt_12_6,0) as mcc_cnt_12_6,
  coalesce(e.mcc_cnt_6,0) as mcc_cnt_6,
  coalesce(e.mcc_cnt_3,0) as mcc_cnt_3,
  coalesce(f.gold_card_hist_flg,0) as gold_card_hist_flg,
  coalesce(f.cls_std_card_hist_flg,0) as cls_std_card_hist_flg,
  coalesce(f.gold_card_hist_cnt,0) as gold_card_hist_cnt,
  coalesce(f.cls_std_card_hist_cnt,0) as cls_std_card_hist_cnt,
  coalesce(f.up_mass_card_flg,0) as up_mass_card_flg,
  coalesce(f.mass_card_flg,0) as mass_card_flg,
  coalesce(f.excl_card_flg,0) as excl_card_flg,
  coalesce(f.mid_card_flg,0) as mid_card_flg,
  coalesce(f.up_mass_card_cnt,0) as up_mass_card_cnt,
  coalesce(f.mass_card_cnt,0) as mass_card_cnt,
  coalesce(f.excl_card_cnt,0) as excl_card_cnt,
  coalesce(f.mid_card_cnt,0) as mid_card_cnt,
  coalesce(g.last_up_mass_flg,0) as last_up_mass_flg, 
  coalesce(g.last_mass_flg,0) as last_mass_flg, 
  coalesce(g.last_middle_flg,0) as last_middle_flg,
 coalesce(h.gold_txn_amt_3m,0) as gold_txn_amt_3m,
 coalesce(h.gold_txn_amt_6m,0) as gold_txn_amt_6m,
 coalesce(h.gold_txn_amt_12m,0) as gold_txn_amt_12m,
 coalesce(h.gold_txn_amt_24m,0) as gold_txn_amt_24m,
 coalesce(h.gold_txn_amt_36m,0) as gold_txn_amt_36m,
 coalesce(h.std_txn_amt_3m,0) as std_txn_amt_3m,
 coalesce(h.std_txn_amt_6m,0) as std_txn_amt_6m,
 coalesce(h.std_txn_amt_12m,0) as std_txn_amt_12m,
 coalesce(h.std_txn_amt_24m,0) as std_txn_amt_24m,
 coalesce(h.std_txn_amt_36m,0) as std_txn_amt_36m

  
from SLV_CS_NEW_VV_NT a 
left join
TMP_SLV_CS_NEW_S19 b 
on a.idclient = b.idclient
  and a.idblank = b.idblank
  and a.contact_dt = b.contact_dt
  
left join 
TMP_SLV_CS_NEW_S21 c
on a.idclient = c.idclient
  and a.idblank = c.idblank
  and a.contact_dt = c.contact_dt
  
left join 
TMP_SLV_CS_NEW_S22 d
on a.idclient = d.idclient
  and a.idblank = d.idblank
  and a.contact_dt = d.contact_dt

left join 
TMP_SLV_CS_NEW_S23 e
on a.idclient = e.idclient
  and a.idblank = e.idblank
  and a.contact_dt = e.contact_dt
  
left join 
TMP_SLV_CS_NEW_S24 f
on a.idclient = f.idclient
  and a.idblank = f.idblank
  and a.contact_dt = f.contact_dt

left join 
TMP_SLV_CS_NEW_S25 g
on a.idclient = g.idclient
  and a.idblank = g.idblank
  and a.contact_dt = g.contact_dt

left join 
TMP_SLV_CS_NEW_S26 h
on a.idclient = h.idclient
  and a.idblank = h.idblank
  and a.contact_dt = h.contact_dt;
  
  -- Разбивка на группы сегментов
 -- drop table TMP_SLV_CS_SEG_11
CREATE TABLE TMP_SLV_CS_SEG_11 AS
select * from  SLV_CS_NEW_VV_NT_V2 sample(50) where segm = 11;
select count(1) from  TMP_SLV_CS_SEG_11
select * from  TMP_SLV_CS_SEG_11

-- drop table TMP_SLV_CS_SEG_8_10
CREATE TABLE TMP_SLV_CS_SEG_8_10 AS
select * from  SLV_CS_NEW_VV_NT_V2 sample(50) where segm in (8,9,10);
select count(1) from  TMP_SLV_CS_SEG_8_10
select * from  TMP_SLV_CS_SEG_8_10


select * from TMP_SLV_CS_SEG_11;
CREATE TABLE TMP_SLV_CS_SEG_11_2 AS
select * from TMP_SLV_CS_SEG_11
where contact_dt between date'2017-08-26' and date'2017-09-20'
;

--drop table TMP_SLV_CS_NEW_VV_1
select * from TMP_SLV_CS_NEW_V2_3;
CREATE TABLE TMP_SLV_CS_NEW_V2_11 AS
select * from SLV_CS_NEW_VV_NT_V2
where contact_dt between date'2017-07-31' and date'2017-08-25'
;
--drop table TMP_SLV_CS_NEW_VV_2
CREATE TABLE TMP_SLV_CS_NEW_V2_2 AS
select * from SLV_CS_NEW_VV_NT_V2 
where contact_dt between date'2017-08-26' and date'2017-09-20'
;

--drop table TMP_SLV_CS_NEW_VV_3
CREATE TABLE TMP_SLV_CS_NEW_V2_3 AS
select * from SLV_CS_NEW_VV_NT_V2
where contact_dt >= date'2017-09-21'
;
--select count(MNTH_FROM_LAST_POS_TXN) from SLV_CS_NEW_VV_NT_V2 where MNTH_FROM_LAST_POS_TXN = '-1'

update SLV_CS_NEW_VV_NT_V2
set MNTH_FROM_LAST_POS_TXN = '60'
where MNTH_FROM_LAST_POS_TXN = '-1';

update TMP_SLV_CS_NEW_V2_11
set MNTH_FROM_LAST_POS_TXN = '60'
where MNTH_FROM_LAST_POS_TXN = '-1';

UPDATE TMP_SLV_CS_NEW_V2_2
set MNTH_FROM_LAST_POS_TXN = '60'
where MNTH_FROM_LAST_POS_TXN = '-1';

select * from TMP_SLV_CS_NEW_V2_2

UPDATE TMP_SLV_CS_NEW_V2_3
set MNTH_FROM_LAST_POS_TXN = '60'
where MNTH_FROM_LAST_POS_TXN = '-1';
/*select count(*), sum(event) from TMP_SLV_CS_NEW_VV UNION
select count(*), sum(event) from TMP_SLV_CS_NEW_VV_1 UNION
select count(*), sum(event) from TMP_SLV_CS_NEW_VV_2 UNION
select count(*), sum(event) from TMP_SLV_CS_NEW_VV_3 
select * from TMP_SLV_CS_NEW_VV_2*/

----------Short list
CREATE TABLE TMP_SLV_SL AS
SELECT
CNT_ENTRY_6M,
DC_CUR_BAL,
INDUSTRY_CD,
MNTH_FROM_LAST_POS_TXN,
MNTH_FROM_LAST_PROD,
from TMP_SLV_CS_NEW_VV_2;

--select distinct(MNTH_FROM_LAST_PROD) from TMP_SLV_CS_NEW_V order by MNTH_FROM_LAST_PROD desc

--drop table SLV_CS_VV_2_SB
CREATE TABLE TMP_SLV_CS_NEW_V2_2_SB AS
select
idclient,
event,
case when CARD_CURR_CNT < 1 then 42
     when CARD_CURR_CNT >= 1 and CARD_CURR_CNT < 2 then 39
     when CARD_CURR_CNT >= 2 and CARD_CURR_CNT < 3 then 34
     when CARD_CURR_CNT >= 3 and CARD_CURR_CNT < 4 then 32
     when CARD_CURR_CNT >= 4 then 28 
end as SB_CARD_CURR_CNT,

case when MNTH_FROM_LAST_POS_TXN < 7 then 29
     when MNTH_FROM_LAST_POS_TXN >= 7 and MNTH_FROM_LAST_POS_TXN < 13 then 31
     when MNTH_FROM_LAST_POS_TXN >= 13 and MNTH_FROM_LAST_POS_TXN < 21 then 35
     when MNTH_FROM_LAST_POS_TXN >= 21 and MNTH_FROM_LAST_POS_TXN < 46 then 36
     when MNTH_FROM_LAST_POS_TXN >= 46 then 39
end as SB_MNTH_FROM_LAST_POS_TXN,

case when INDUSTRY_CD in (-1, 1, 11, 13, 19, 2, 21, 26, 30, 31, 32) then 22
     when INDUSTRY_CD in (10, 14, 17, 22, 27, 6, 8) then 27
     when INDUSTRY_CD in (20, 23, 24, 29, 7, 9)then 29
     when INDUSTRY_CD in (12, 16, 18, 25, 28, 3, 4) then 34
     when INDUSTRY_CD in (0, 15, 5) or INDUSTRY_CD = null then 49
end as SB_INDUSTRY_CD,

case when MAX_CAT_MCC_12M in (1, 10, 11, 13, 16, 19, 21, 3, 4, 5) then 30
     when MAX_CAT_MCC_12M in (15, 17, 7, 8) then 34
     when MAX_CAT_MCC_12M in (0, 12, 18, 2, 20, 6, 9, 14) or MAX_CAT_MCC_12M = null then 39
end as SB_MAX_CAT_MCC_12M,

case when MNTH_FROM_LAST_PROD < 17 then 28
     when MNTH_FROM_LAST_PROD >= 17 and MNTH_FROM_LAST_PROD < 37 then 38
     when MNTH_FROM_LAST_PROD >= 37 and MNTH_FROM_LAST_PROD < 68 then 42
     when MNTH_FROM_LAST_PROD >= 68 then 45
end as SB_MNTH_FROM_LAST_PROD
     
from TMP_SLV_CS_NEW_V2_2
;

--select * from TMP_SLV_CS_NEW_V2_2_SB;
--drop table GAG_CRED_CS_VV_2_P
CREATE TABLE  TMP_SLV_CS_VV_2_P AS
select
  a.*,
    cast (1/(1 +exp(-(3.0357
    +(-0.0335*a.SB_CARD_CURR_CNT)
    +(-0.0351*a.SB_INDUSTRY_CD)   
    +(-0.0376*a.SB_MAX_CAT_MCC_12M)
    +(-0.0327*a.SB_MNTH_FROM_LAST_POS_TXN)
    +(-0.0347*a.SB_MNTH_FROM_LAST_PROD)
    ))) as decimal (18,2)) as score
 from TMP_SLV_CS_NEW_V2_2_SB a
;
--select distinct(SB_DC_CUR_BAL) from SLV_CS_VV_2_SB
--select distinct(SB_DC_CUR_BAL) from SLV_CS_VV_2_P
--drop table TMP_SLV_CS_VV_1_SB
CREATE TABLE TMP_SLV_CS_VV_1_SB AS
select
idclient,
event,
case when CARD_CURR_CNT < 1 then 42
     when CARD_CURR_CNT >= 1 and CARD_CURR_CNT < 2 then 39
     when CARD_CURR_CNT >= 2 and CARD_CURR_CNT < 3 then 34
     when CARD_CURR_CNT >= 3 and CARD_CURR_CNT < 4 then 32
     when CARD_CURR_CNT >= 4 then 28 
end as SB_CARD_CURR_CNT,

case when MNTH_FROM_LAST_POS_TXN < 7 then 29
     when MNTH_FROM_LAST_POS_TXN >= 7 and MNTH_FROM_LAST_POS_TXN < 13 then 31
     when MNTH_FROM_LAST_POS_TXN >= 13 and MNTH_FROM_LAST_POS_TXN < 21 then 35
     when MNTH_FROM_LAST_POS_TXN >= 21 and MNTH_FROM_LAST_POS_TXN < 46 then 36
     when MNTH_FROM_LAST_POS_TXN >= 46 then 39
end as SB_MNTH_FROM_LAST_POS_TXN,

case when INDUSTRY_CD in (-1, 1, 11, 13, 19, 2, 21, 26, 30, 31, 32) then 22
     when INDUSTRY_CD in (10, 14, 17, 22, 27, 6, 8) then 27
     when INDUSTRY_CD in (20, 23, 24, 29, 7, 9)then 29
     when INDUSTRY_CD in (12, 16, 18, 25, 28, 3, 4) then 34
     when INDUSTRY_CD in (0, 15, 5) or INDUSTRY_CD = null then 49
end as SB_INDUSTRY_CD,

case when MAX_CAT_MCC_12M in (1, 10, 11, 13, 16, 19, 21, 3, 4, 5) then 30
     when MAX_CAT_MCC_12M in (15, 17, 7, 8) then 34
     when MAX_CAT_MCC_12M in (0, 12, 18, 2, 20, 6, 9, 14) or MAX_CAT_MCC_12M = null then 39
end as SB_MAX_CAT_MCC_12M,

case when MNTH_FROM_LAST_PROD < 17 then 28
     when MNTH_FROM_LAST_PROD >= 17 and MNTH_FROM_LAST_PROD < 37 then 38
     when MNTH_FROM_LAST_PROD >= 37 and MNTH_FROM_LAST_PROD < 68 then 42
     when MNTH_FROM_LAST_PROD >= 68 then 45
end as SB_MNTH_FROM_LAST_PROD
from TMP_SLV_CS_NEW_V2_11
;

--drop table GAG_CRED_CS_VV_1_P
CREATE TABLE  TMP_SLV_CS_VV_1_P AS
  select a.*,
    cast (1/(1 +exp(-(3.0357
    +(-0.0335*a.SB_CARD_CURR_CNT)
    +(-0.0351*a.SB_INDUSTRY_CD)   
    +(-0.0376*a.SB_MAX_CAT_MCC_12M)
    +(-0.0327*a.SB_MNTH_FROM_LAST_POS_TXN)
    +(-0.0347*a.SB_MNTH_FROM_LAST_PROD)
    ))) as decimal (18,2)) as score
 from TMP_SLV_CS_VV_1_SB a
;

--drop table TMP_SLV_CS_VV_3_SB
CREATE TABLE TMP_SLV_CS_VV_3_SB AS
select
idclient,
event,
case when CARD_CURR_CNT < 1 then 42
     when CARD_CURR_CNT >= 1 and CARD_CURR_CNT < 2 then 39
     when CARD_CURR_CNT >= 2 and CARD_CURR_CNT < 3 then 34
     when CARD_CURR_CNT >= 3 and CARD_CURR_CNT < 4 then 32
     when CARD_CURR_CNT >= 4 then 28 
end as SB_CARD_CURR_CNT,

case when MNTH_FROM_LAST_POS_TXN < 7 then 29
     when MNTH_FROM_LAST_POS_TXN >= 7 and MNTH_FROM_LAST_POS_TXN < 13 then 31
     when MNTH_FROM_LAST_POS_TXN >= 13 and MNTH_FROM_LAST_POS_TXN < 21 then 35
     when MNTH_FROM_LAST_POS_TXN >= 21 and MNTH_FROM_LAST_POS_TXN < 46 then 36
     when MNTH_FROM_LAST_POS_TXN >= 46 then 39
end as SB_MNTH_FROM_LAST_POS_TXN,

case when INDUSTRY_CD in (-1, 1, 11, 13, 19, 2, 21, 26, 30, 31, 32) then 22
     when INDUSTRY_CD in (10, 14, 17, 22, 27, 6, 8) then 27
     when INDUSTRY_CD in (20, 23, 24, 29, 7, 9)then 29
     when INDUSTRY_CD in (12, 16, 18, 25, 28, 3, 4) then 34
     when INDUSTRY_CD in (0, 15, 5) or INDUSTRY_CD = null then 49
end as SB_INDUSTRY_CD,

case when MAX_CAT_MCC_12M in (1, 10, 11, 13, 16, 19, 21, 3, 4, 5) then 30
     when MAX_CAT_MCC_12M in (15, 17, 7, 8) then 34
     when MAX_CAT_MCC_12M in (0, 12, 18, 2, 20, 6, 9,14) or MAX_CAT_MCC_12M = null then 39
end as SB_MAX_CAT_MCC_12M,

case when MNTH_FROM_LAST_PROD < 17 then 28
     when MNTH_FROM_LAST_PROD >= 17 and MNTH_FROM_LAST_PROD < 37 then 38
     when MNTH_FROM_LAST_PROD >= 37 and MNTH_FROM_LAST_PROD < 68 then 42
     when MNTH_FROM_LAST_PROD >= 68 then 45
end as SB_MNTH_FROM_LAST_PROD
from TMP_SLV_CS_NEW_V2_3
;

--drop table GAG_CRED_CS_VV_3_P
CREATE TABLE  TMP_SLV_CS_VV_3_P AS
select
  a.*,
    cast (1/(1 +exp(-(3.0357
    +(-0.0335*a.SB_CARD_CURR_CNT)
    +(-0.0351*a.SB_INDUSTRY_CD)   
    +(-0.0376*a.SB_MAX_CAT_MCC_12M)
    +(-0.0327*a.SB_MNTH_FROM_LAST_POS_TXN)
    +(-0.0347*a.SB_MNTH_FROM_LAST_PROD)
    ))) as decimal (18,2)) as score
 from TMP_SLV_CS_VV_3_SB a
;
--drop table GAG_CRED_CS_VV

CREATE TABLE SLV_CS_VV_SB AS
select
idclient,
event,
case when CARD_CURR_CNT < 1 then 42
     when CARD_CURR_CNT >= 1 and CARD_CURR_CNT < 2 then 39
     when CARD_CURR_CNT >= 2 and CARD_CURR_CNT < 3 then 34
     when CARD_CURR_CNT >= 3 and CARD_CURR_CNT < 4 then 32
     when CARD_CURR_CNT >= 4 then 28 
end as SB_CARD_CURR_CNT,

case when MNTH_FROM_LAST_POS_TXN < 7 then 29
     when MNTH_FROM_LAST_POS_TXN >= 7 and MNTH_FROM_LAST_POS_TXN < 13 then 31
     when MNTH_FROM_LAST_POS_TXN >= 13 and MNTH_FROM_LAST_POS_TXN < 21 then 35
     when MNTH_FROM_LAST_POS_TXN >= 21 and MNTH_FROM_LAST_POS_TXN < 46 then 36
     when MNTH_FROM_LAST_POS_TXN >= 46 then 39
end as SB_MNTH_FROM_LAST_POS_TXN,

case when INDUSTRY_CD in (-1, 1, 11, 13, 19, 2, 21, 26, 30, 31, 32) then 22
     when INDUSTRY_CD in (10, 14, 17, 22, 27, 6, 8) then 27
     when INDUSTRY_CD in (20, 23, 24, 29, 7, 9)then 29
     when INDUSTRY_CD in (12, 16, 18, 25, 28, 3, 4) then 34
     when INDUSTRY_CD in (0, 15, 5) or INDUSTRY_CD = null then 49
end as SB_INDUSTRY_CD,

case when MAX_CAT_MCC_12M in (1, 10, 11, 13, 16, 19, 21, 3, 4, 5) then 30
     when MAX_CAT_MCC_12M in (15, 17, 7, 8) then 34
     when MAX_CAT_MCC_12M in (0, 12, 18, 2, 20, 6, 9, 14) then  39
end as SB_MAX_CAT_MCC_12M,

case when MNTH_FROM_LAST_PROD < 17 then 28
     when MNTH_FROM_LAST_PROD >= 17 and MNTH_FROM_LAST_PROD < 37 then 38
     when MNTH_FROM_LAST_PROD >= 37 and MNTH_FROM_LAST_PROD < 68 then 42
     when MNTH_FROM_LAST_PROD >= 68 then 45
end as SB_MNTH_FROM_LAST_PROD
     
from SLV_CS_NEW_VV_NT_V2
;
--drop table GAG_CRED_CS_VV_P
CREATE TABLE  SLV_CS_VV_P AS SLV_CS_VV_P
select
  a.*,
    cast (1/(1 +exp(-(3.0357
    +(-0.0335*a.SB_CARD_CURR_CNT)
    +(-0.0351*a.SB_INDUSTRY_CD)   
    +(-0.0376*a.SB_MAX_CAT_MCC_12M)
    +(-0.0327*a.SB_MNTH_FROM_LAST_POS_TXN)
    +(-0.0347*a.SB_MNTH_FROM_LAST_PROD)
    ))) as decimal (18,2)) as score
 from SLV_CS_VV_SB a
;

-- Заполнение отчёта (Факт-прогноз)
select 
case        when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end forecast,
sum(case when event = '1' then 1 end)*100/count(distinct idclient) as fact_prc,
count(distinct idclient) as client_cnt,
--sum(case when event = '1' then 1 end) as event_cnt,
count(distinct idclient)*100/(select count(distinct idclient) from SLV_CS_VV_P) as segment_part
from SLV_CS_VV_P
group by case when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end
union
select 'Total' as forecast, 
    sum(case when event = '1' then 1 end)*100/count(distinct idclient) as fact_prc,
count(distinct idclient) as client_cnt,
1*100 as segment_part from SLV_CS_VV_P

--Заполнение отчёта в разрезе переменных - трендовость
--SB_CARD_CURR_CNT
select 
case        when SB_CARD_CURR_CNT = 42
          then '1' 
            when SB_CARD_CURR_CNT = 39
          then '2'
            when SB_CARD_CURR_CNT = 34  
          then '3'
            when SB_CARD_CURR_CNT = 32  
          then '4'   
            when SB_CARD_CURR_CNT = 28
          then '5'          
            else null end group_num,
sum(case when event = '1' then 1 end)*100/count(idclient) as event_per,
count(idclient) as client_cnt
from SLV_CS_VV_P 
group by case 
          when SB_CARD_CURR_CNT = 42
          then '1' 
            when SB_CARD_CURR_CNT = 39
          then '2'
            when SB_CARD_CURR_CNT = 34  
          then '3'
            when SB_CARD_CURR_CNT = 32  
          then '4'   
            when SB_CARD_CURR_CNT = 28
          then '5' 
            else null end 
order by group_num asc

--SB_MNTH_FROM_LAST_POS_TXN
select 
case        when SB_MNTH_FROM_LAST_POS_TXN = 29
          then '1' 
            when SB_MNTH_FROM_LAST_POS_TXN = 31
          then '2'
            when SB_MNTH_FROM_LAST_POS_TXN = 35  
          then '3'
            when SB_MNTH_FROM_LAST_POS_TXN = 36  
          then '4'
            when SB_MNTH_FROM_LAST_POS_TXN = 39  
          then '5'
            else null end group_n,
sum(case when event = '1' then 1 end)*100/count(idclient) as event_per,
count(idclient) as client_cnt
from SLV_CS_VV_P 
group by case 
             when SB_MNTH_FROM_LAST_POS_TXN = 29
          then '1' 
            when SB_MNTH_FROM_LAST_POS_TXN = 31
          then '2'
            when SB_MNTH_FROM_LAST_POS_TXN = 35  
          then '3'
            when SB_MNTH_FROM_LAST_POS_TXN = 36  
          then '4'
            when SB_MNTH_FROM_LAST_POS_TXN = 39  
          then '5'
            else null end 
order by group_n asc

--MNTH_FROM_LAST_PROD

select 
case        when SB_MNTH_FROM_LAST_PROD = 28
          then '1' 
            when SB_MNTH_FROM_LAST_PROD = 38
          then '2'
            when SB_MNTH_FROM_LAST_PROD = 42  
          then '3'
            when SB_MNTH_FROM_LAST_PROD = 45  
          then '4'
            else null end group_n,
sum(case when event = '1' then 1 end)*100/count(idclient) as event_per,
count(idclient) as client_cnt
from SLV_CS_VV_P 
group by case 
            when SB_MNTH_FROM_LAST_PROD = 28
          then '1' 
            when SB_MNTH_FROM_LAST_PROD = 38
          then '2'
            when SB_MNTH_FROM_LAST_PROD = 42  
          then '3'
            when SB_MNTH_FROM_LAST_PROD = 45  
          then '4'
            else null end 
order by group_n asc
--SSI
select V1.forecast,segment_part_v1,segment_part_b2,segment_part_v3 from
(select
case        when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end forecast,
count(distinct idclient)*100/(select count(distinct idclient) from TMP_SLV_CS_VV_1_P) as segment_part_v1
from TMP_SLV_CS_VV_1_P 
group by case when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end) V1
left join
(select 
case        when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end forecast,
count(distinct idclient)*100/(select count(distinct idclient) from TMP_SLV_CS_VV_2_P) as segment_part_b2
from TMP_SLV_CS_VV_2_P 
group by case when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end) b2
on V1.forecast=b2.forecast

left join
(select 
case        when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end forecast,
count(distinct idclient)*100/(select count(distinct idclient) from TMP_SLV_CS_VV_3_P) as segment_part_v3
from TMP_SLV_CS_VV_3_P 
group by case when score between 0 and 0.04  
          then '0-0,04' 
            when score between 0.05 and 0.09  
          then '0,05-0,09'
            when score between 0.1 and 0.14  
          then '0,1-0,14'
            when score between 0.15 and 0.19  
          then '0,15-0,19'
            when score between 0.2 and 0.24  
          then '0,2-0,24'
            when score between 0.25 and 0.3  
          then '0,25-0,3'
            else null end) V3
on V1.forecast=V3.forecast
order by forecast asc
