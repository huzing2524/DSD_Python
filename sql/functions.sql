---------------------------------------------------------------------------------------------------------------

---------------------     version 3.5.0 update 2019/04/23      -------------------------------------------------

----------------------------------------------------------------------------------------------------------------

-- 查询不同状态的订单
create or replace FUNCTION get_order_state_counts(fac varchar(20)) returns RECORD  AS $$
DECLARE
  ret RECORD;
begin
  SELECT (select count(1) :: integer
          from base_orders
          where factory = fac
            and state = '1'),
         (select count(1) :: integer
          from base_orders
          where factory = fac
            and state = '2'),
         (select count(1) :: integer
          from base_orders
          where factory = fac
            and state = '3'),
         (select count(1) :: integer
          from base_orders
          where factory = fac
            and state = '6')
         INTO ret;
  RETURN ret;
END;
$$ LANGUAGE plpgsql;


-- 采购单审核通过后，生成对应供应商的生产单
CREATE OR REPLACE FUNCTION public.purchase_create_order(purchase_id varchar(36), order_id varchar(36))
 RETURNS void
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    r base_purchase_materials%ROWTYPE;
    c integer;
    fac_id varchar(36);
    client_id varchar(36);
    plan_arrival_time integer;
    remark varchar(60);
BEGIN
    raise notice 'order_id: %', order_id;
    select t2.id, t1.factory,t1.plan_arrival_time, t1.remark into fac_id, client_id, plan_arrival_time,remark from base_purchases t1 left join factorys t2
     on t1.supplier_id = t2.id  where t1.id = purchase_id and t2.id notnull;
	raise notice 'fac_id: %', c;
    if fac_id notnull then
        EXECUTE format('insert into base_orders (id, factory, client_id, create_time, order_type, plan_arrival_time,
         purchase_id, remark) values (%L, %L, %L, extract(epoch from now()) ::numeric::integer ,%L, %L, %L, %L);',
				 order_id, fac_id, client_id, '2', plan_arrival_time, purchase_id, remark);
		FOR r IN
		    SELECT *
		    FROM base_purchase_materials t where t.purchase_id = purchase_id
		loop
			EXECUTE format('
		    insert into base_order_products (order_id, product_id, product_count,unit_price) values (%L,%L,%L,%L) ;
		     ', order_id, r.product_id, r.product_count, r.unit_price);
		END LOOP;
	end if;
END;
$function$;



-- 将现有客户资源导入到资源池
CREATE OR REPLACE FUNCTION import_clients_pool()
  RETURNS void AS
$BODY$
DECLARE
    r RECORD;
BEGIN
		FOR r IN
			select
				t1.id,
				t1.name as contacts,
				coalesce(t2.contact, '') as phone,
				coalesce(t1.title,
					'' ) as name,
				coalesce(t2.name, '') as name,
				coalesce(t2.industry, '') as industry,
				coalesce(t2.region, '') as region,
				coalesce(t2."time", extract(epoch from now()) ::numeric::integer) as time
			from
				(
				select
					id,
					coalesce(( array_agg(title))[1],
					'' ) as title,
					coalesce(( array_agg(name))[1],
					'' ) as name,
					administrators
				from
					(
					select
						t1.id,
						t2.name,
						t1.administrators,
						t1.title
					from
						factorys t1
					left join user_info t2 on
						array[t2.phone] <@ t1.administrators ) t
				group by
					id,
					administrators ) t1
			left join bg_examine t2 on
				t1.administrators @> array[t2.id]
		loop
			EXECUTE format('
		    insert into base_clients_pool values (%L,%L,%L,%L,%L,%L,%L,%L, %L) ;
		     ', r.id, r.name, r.contacts, r.phone, '', r.industry, r.time, r.region, '');
		end LOOP;
END;
$BODY$ LANGUAGE plpgsql;
select import_clients_pool();


-- update 2019.05.17 version 3.5.1
-- 将订单状态导入到新的表当中
CREATE OR REPLACE FUNCTION import_order_stats()
  RETURNS void AS
$BODY$
DECLARE
    r RECORD;
BEGIN
		FOR r IN
			select id, state, order_type, create_time, creator, approval_time, pause_time, cancel_time, remark from base_orders
		loop
			-- create action
			EXECUTE format('
		    insert into base_orders_stats (order_id, state, remark, operator, optime) values (%L, %L, %L,%L,%L) ;
		     ', r.id, '1', r.remark, r.creator, r.create_time);
		    if r.approval_time > 0 then
		        EXECUTE format('insert into base_orders_stats (order_id, state, remark, operator, optime) values (%L, %L, %L,%L,%L) ;
		     ', r.id, '2', '', r.creator, r.approval_time);
		    end if;
		    if r.cancel_time > 0 then
		    	EXECUTE format('insert into base_orders_stats (order_id, state, remark, operator, optime) values (%L, %L, %L,%L,%L) ;
		     ', r.id, '3', '', r.creator, r.cancel_time);
		     end if;
		     if r.pause_time > 0 then
		    	EXECUTE format('insert into base_orders_stats (order_id, state, remark, operator, optime) values (%L, %L, %L,%L,%L) ;
		     ', r.id, '4', '', r.creator, r.pause_time);
		     end if;
		end LOOP;
END;
$BODY$ LANGUAGE plpgsql;
select import_order_stats();