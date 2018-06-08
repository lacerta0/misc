create or replace procedure U_RPT_AVLBL_LEADS_SC1
  (
   ic_e_mail         in varchar2 default nvl(crm_user.f_crm_employee_email,'lv-soina@rsb.ru'),
   in_trace          in number   default 0,                                         --признак трассировки 0/1
   in_stop_procedure in number   default 0                                          --не смотреть признак остановки процедуры
  ) authid current_user as

  lc_process varchar2(100) := 'U_RPT_AVLBL_LEADS_SC1';        --Наименование процесса для лога
  lc_sql     varchar2(20000);                                      --Текст SQL запрос
  ln_step    number := 0;                                          --Номер шага
  ln_lastId  number;                                               --Ссылка в лог на текущий процесс
  lc_error   varchar2(1000);                                       --Текст ошибки
  --Временные таблицы
TMP_SLV_ProbDistr1         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr4         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr5         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr6         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr7         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr8         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr11         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
TMP_SLV_ProbDistr12         varchar2(30) := crm_user.f_sys_tmp_table(ic_process => lc_process);
---TMP_SLV_ProbDistr1 varchar2(30):= 'TMPNAme';
begin

  --проверка на отсутствие флажка, что не нужно запускать процедуру
  if 1 != f_check_start_job(ic_process => lc_process,in_stop_procedure => in_stop_procedure,ic_e_mail => ic_e_mail,in_trace => in_trace,in_no_log => 1)
    then goto proc_error;
  end if;

  --Запись в лог, что процесс запущен
  ln_step := ln_step + 1;
  ln_lastId := Insertlog(ic_process => lc_process,ic_status => 'START',ic_step => to_char(ln_step,'000'),ic_email => ic_e_mail);

  --карта1
  ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr1||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from crm_user.score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_pil s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_pil v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 1
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr1,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC1';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC1 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr1||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);

   --карта4
  ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr4||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from crm_user.score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_pil s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_pil v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 4
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr4,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC4';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC4 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr4||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
--карта 5
 ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr5||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_cs_client s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_card v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 5
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr5,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC5';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC5 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr5||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
--карта 6
 ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr6||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_cs_client s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_card v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 6
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr6,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC6';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC6 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr6||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
--карта 8
 ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr8||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_cs_client s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_card v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 8
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr8,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC8';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC8 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr8||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
--карта 7
 ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr7||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from crm_user.score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_pil s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_pil v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 7
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr7,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC7';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC7 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr7||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
---карта 11
 ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr11||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from crm_user.score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_pil s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_pil v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 11
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr11,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC11';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC11 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr11||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
---карта 12

 ln_step := ln_step + 1;
  lc_sql := '
create table '||TMP_SLV_ProbDistr12||' as
select /*+ use_hash(scc,e,s,m,v) */

case        when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end prob_dist,
       count(scc.idclient) cnt
       ,sum(case when e.idclient is not null then 1 end) is_excl
       ,sum(case when s.idclient is not null then 1 end) is_stop
       ,sum(case when m.idclient is not null then 1 end) is_mob
       ,sum(case when v.idclient is not null then 1 end) is_vitr
       ,sum(case when e.idclient is null
                      and s.idclient is null
                      and m.idclient is not null
                      and v.idclient is null
                   then 1 end) is_available
from crm_user.score_card_clients scc
     left join (select distinct e.idclient from exclude_sale_clients e where sysdate between e.df and e.dt) e
          on scc.idclient = e.idclient
     left join stop_list_pil s
          on scc.idclient = s.idclient
     left join (select distinct idclient from crm_client_mobile) m
          on scc.idclient = m.idclient
     left join (select distinct v.idclient from vitr_road_pil v) v
          on scc.idclient = v.idclient
where scc.score_card_id = 12
and sysdate between scc.df and scc.dt
group by case when scc.score between 0 and 0.04
          then ''0-0,04''
            when scc.score between 0.05 and 0.09
          then ''0,05-0,09''
            when scc.score between 0.1 and 0.14
          then ''0,1-0,14''
            when scc.score between 0.15 and 0.19
          then ''0,15-0,19''
            when scc.score between 0.2 and 0.24
          then ''0,2-0,24''
            when scc.score between 0.25 and 0.29
          then ''0,25-0,29''
            when scc.score between 0.3 and 0.34
          then ''0,3-0,34''
            when scc.score between 0.35 and 0.39
          then ''0,35-0,39''
            when scc.score between 0.4 and 0.44
          then ''0,4-0,44''
            when scc.score between 0.45 and 0.49
          then ''0,45-0,49''
            when scc.score between 0.5 and 0.54
          then ''0,5-0,54''
            when scc.score between 0.55 and 0.59
          then ''0,55-0,59''
            when scc.score between 0.6 and 0.64
          then ''0,6-0,64''
            when scc.score between 0.65 and 0.69
          then ''0,65-0,69''
            when scc.score between 0.7 and 0.74
          then ''0,7-0,74''
            when scc.score between 0.75 and 0.79
          then ''0,75-0,79''
            when scc.score between 0.8 and 0.84
          then ''0,8-0,84''
            when scc.score between 0.85 and 0.89
          then ''0,85-0,89''
            when scc.score between 0.9 and 0.94
          then ''0,9-0,94''
            when scc.score between 0.95 and 1
          then ''0,95-1''
            else null end
';
  sys_sql_create(ic_target => TMP_SLV_ProbDistr12,ic_process => lc_process,ic_step => to_char(ln_step,'000'),ic_sql => lc_sql,ic_e_mail => ic_e_mail,in_trace => in_trace);
   ln_step := ln_step + 1;
  lc_sql := 'truncate table RPT_AVLBL_LEADS_SC12';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);
ln_step := ln_step + 1;
  lc_sql := 'insert into RPT_AVLBL_LEADS_SC12 (prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available)
   select prob_dist, cnt, is_excl,is_stop,is_mob,is_vitr,is_available from '||TMP_SLV_ProbDistr12||'
   ';
  sys_sql_execute(ic_process => lc_process, ic_step => to_char(ln_step, '000'), ic_sql => lc_sql, ic_e_mail => ic_e_mail, in_trace => in_trace);


  if in_trace = 0 then
    sys_drop_table(ic_target => TMP_SLV_ProbDistr1,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr4,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr5,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr6,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr7,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr8,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr11,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);
    sys_drop_table(ic_target => TMP_SLV_ProbDistr12,ic_process => lc_process,in_trace => in_trace,ic_e_mail => ic_e_mail);                
  end if;

  UpdateLog(ic_status => 'OK',id_dt => sysdate,in_error => lc_error,in_id => ln_lastId);

  commit;

  send_mail(ic_Recipient => ic_e_mail,ic_subj => lc_process,ic_msg => 'Done');

  <<proc_error>>

  null;

exception
  when others then
    if in_trace != 0 then
      DBMS_OUTPUT.put_line(chr(10));
      DBMS_OUTPUT.put_line('Error');
      DBMS_OUTPUT.put_line(chr(10));
    end if;

    lc_error := substr(SQLERRM, 1, 950);
    UpdateLog(ic_status => 'ERROR',id_dt => sysdate,in_error => lc_error,in_id => ln_lastId);
    Send_mail(ic_Recipient => ic_e_mail,
              ic_mail_list_copy => 'crm_err@rsb.ru',
              ic_subj => 'ERROR ic_process => '||lc_process||'; ic_step => '||to_char(ln_step, '000'),
              ic_msg => lc_error);

end;
/
