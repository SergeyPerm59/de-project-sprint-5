--create table l_user_group_activity
create table SERGEYPERM89YANDEXRU__DWH.l_user_group_activity
(
    hk_l_user_group_activity int PRIMARY KEY,
    hk_user_id			     int not null CONSTRAINT fk_l_user_group_activity_users  REFERENCES SERGEYPERM89YANDEXRU__DWH.h_users(hk_user_id),
    hk_group_id              int not null CONSTRAINT fk_l_user_group_activity_groups REFERENCES SERGEYPERM89YANDEXRU__DWH.h_groups(hk_group_id),
    load_dt                  datetime,
    load_src   		         varchar(20)
)
order by load_dt
SEGMENTED BY HASH(hk_l_user_group_activity) ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
 
--insert into l_user_group_activity
INSERT INTO SERGEYPERM89YANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
hash(hu.hk_user_id,hg.hk_group_id),
hu.hk_user_id,
hg.hk_group_id,
hu.load_dt::timestamp,
hu.load_src 
from SERGEYPERM89YANDEXRU__STAGING.group_log as gl
left join SERGEYPERM89YANDEXRU__DWH.h_users hu on gl.user_id = hu.user_id 
left join SERGEYPERM89YANDEXRU__DWH.h_groups hg on gl.group_id = hg.group_id; 

--create satellit s_auth_history
create table SERGEYPERM89YANDEXRU__DWH.s_auth_history
(
    hk_l_user_group_activity int not null CONSTRAINT fk_s_auth_history REFERENCES SERGEYPERM89YANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from int,
    "event"   varchar(6),
    event_dt  timestamp,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--INSERT INTO satellit s_auth_history
INSERT INTO SERGEYPERM89YANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select 
luga.hk_l_user_group_activity,
case when gl.event = 'add' then gl.user_id else null end as user_id_from,
gl.event,
gl.datetime as event_dt,
now() as load_dt,
's3' as load_src
from SERGEYPERM89YANDEXRU__STAGING.group_log as gl
left join SERGEYPERM89YANDEXRU__DWH.h_groups as hg 
	on gl.group_id = hg.group_id
left join SERGEYPERM89YANDEXRU__DWH.h_users as hu 
	on gl.user_id = hu.user_id
left join SERGEYPERM89YANDEXRU__DWH.l_user_group_activity as luga 
	on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;
	
--CTE-----------------------------------------------------
with user_group_messages as (
    SELECT 
		lgd.hk_group_id,
		count(case when lgd.hk_message_id is not null then lgd.hk_group_id end) as cnt_users_in_group_with_messages
	from SERGEYPERM89YANDEXRU__DWH.l_groups_dialogs lgd 
	group by lgd.hk_group_id, lgd.hk_message_id
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;


with user_group_log as (
    SELECT 
distinct(luga.hk_user_id),
luga.hk_group_id ,
count(case when sah.event='add' THEN luga.hk_user_id end) as cnt_added_users
from SERGEYPERM89YANDEXRU__DWH.s_auth_history sah 
left join SERGEYPERM89YANDEXRU__DWH.l_user_group_activity luga 
	on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
group by luga.hk_user_id, sah.event
)
select hk_group_id
            ,cnt_added_users
from user_group_messages
order by cnt_added_users
limit 10; 

-- CTE ALL--------------------------------------------------------
with user_group_log as (
select distinct(luga.hk_user_id),
		luga.hk_group_id ,
		count(case when sah.event='add' THEN luga.hk_user_id end) as cnt_added_users
from SERGEYPERM89YANDEXRU__DWH.s_auth_history sah 
left join SERGEYPERM89YANDEXRU__DWH.l_user_group_activity luga 
	on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
group by luga.hk_user_id, sah.event,luga.hk_group_id
)
,user_group_messages as (
    SELECT 
		lgd.hk_group_id,
		count(case when lgd.hk_message_id is not null then lgd.hk_group_id end) as cnt_users_in_group_with_messages
	from SERGEYPERM89YANDEXRU__DWH.l_groups_dialogs lgd 
	group by lgd.hk_group_id, lgd.hk_message_id
)
select * from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages, ugl.cnt_added_users desc 