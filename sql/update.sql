-- version 3.3.0 update 2019/2/21

create table industry_plus_factorys
(
  "phone"          character varying(11) not null,
  "company_name"   character varying(50),
  "industry"       character varying(50),
  "region"         character varying(50),
  "contact_name"   character varying(20),
  "contact_phone"  character varying(11),
  "solve_problems" character varying(30)[] default '{}' :: character varying[],
  "supplement"     character varying(200),
  "time"           integer
);

create table industry_plus_test
(
  "phone"              character varying(11) not null,
  "company_name"       character varying(50) not null,
  "intelligent_degree" character varying(5)[] default '{}' :: character varying[],
  "score"              double precision,
  "time"               integer,
  constraint "industry_plus_test_pkey" primary key ("phone")
);


------------------------------------------------------------------------------------
-- 订单，采购单相关sql (Part1) version 3.5.0 update 2019/04/23
------------------------------------------------------------------------------------------

-- 客户资源池
create table base_clients_pool
(
  id          varchar(36) primary key default uuid_generate_v4(),
  "name"      varchar(50) not null,
  contacts    varchar(20) not null,
  phone       varchar(11) not null,
  "position"  varchar(20) null,
  industry    varchar(50),
  create_time int4,
  region      varchar(50),
  address     varchar(100),
  unique (name)
);

-- 客户表
create table base_clients
(
  id          varchar(36) references base_clients_pool (id),
  factory     varchar(36) not null,
  "name"      varchar(50) not null,
  contacts    varchar(20) not null,
  phone       varchar(11) not null,
  industry    varchar(50),
  "position"  varchar(20) null,
  create_time int4,
  creator     varchar(36),
  region      varchar(50),
  address     varchar(100),
  constraint base_clients_pkey PRIMARY KEY (id, factory),
  constraint fk_factory foreign key (factory) references factorys (id),
  constraint fk_creator foreign key (creator) references user_info (user_id)
);
create index base_clients_factory_index on base_clients using btree (factory);


-- 供应商表
create table base_suppliers
(
  id          varchar(36) references base_clients_pool (id),
  factory     varchar(36) not null,
  "name"      varchar(50) not null,
  contacts    varchar(20) not null,
  phone       varchar(11) not null,
  "position"  varchar(20) null,
  industry    varchar(50),
  create_time int4 default extract(epoch from now())::integer,
  creator     varchar(36),
  region      varchar(50),
  address     varchar(100),
  constraint base_suppliers_pkey PRIMARY KEY (id, factory),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id),
  constraint fk_creator foreign key (creator) REFERENCES user_info (user_id)
);
create index base_suppliers_factory_index on base_suppliers using btree (factory);

create table if not exists base_purchases
(
  id                varchar(36) primary key,
  product_task_id   varchar(36) unique,
  factory           varchar(36) not null,
  supplier_id       varchar(36) null,
  plan_arrival_time integer     default 0,                                  -- 计划送达时间
  approver          varchar(36),                                            -- 审批人
  remark            varchar(60) default '',                                 -- 备注
  create_time       int4        default extract(epoch from now())::integer, -- 订单创建时间
  approval_time     integer     default 0,                                  -- 审批时间
  cancel_time       integer     default 0,                                  -- 取消时间
  creator           varchar(36),                                            -- 创建人
  state             varchar(1)  default '1',                                -- 订单状态 1: 待审批， 2: 已审核待确认 3：采购中, 4: 运输中, 5：已入库, 6:已取消
  constraint fk_supplier foreign key (supplier_id) references base_clients_pool (id),
  constraint fk_factory foreign key (factory) references factorys (id),
  constraint fk_approver foreign key (approver) references user_info (user_id),
  constraint fk_creator foreign key (creator) references user_info (user_id)
);


create table base_orders
(
  id                  varchar(36) primary key,
  factory             varchar(36) not null,
  client_id           varchar(36) null,                                            -- 客户
  collected           float            default 0,                                  -- 已收款金额
  approver            varchar(36),                                                 -- 审批人
  remark              varchar(60)      default '',                                 -- 备注
  create_time         integer          default extract(epoch from now())::integer, -- 订单创建时间
  approval_time       integer          default 0,                                  -- 审批时间
  pause_time          integer          default 0,                                  -- 暂停时间
  cancel_time         integer          default 0,                                  -- 取消时间
  plan_arrival_time   integer          default 0,                                  -- 计划送达时间
  actual_arrival_time integer          default 0,                                  -- 实际送达时间
  deliver_time        integer          default 0,                                  -- 发货时间
  creator             varchar(36),                                                 -- 创建人
  state               varchar(1)       default '1',                                -- 订单状态 1: 待审批， 2：待发货, 3: 运输中, 4: 已送达，5：已取消, 6:已暂停
  before_pause_state  varchar(1),                                                  -- 暂停前，订单状态
  order_type          varchar(1)       default '1',                                -- 订单类型 1：自建订单， 2：推送订单
  purchase_id         varchar(36),                                                 -- 采购单号, (该订单是有此采购单生成)
  contract            varchar(20) null default ''::character varying,
  del                 varchar(1)       default '0',                                -- 该订单是否已删除
  constraint fk_client foreign key (client_id) references base_clients_pool (id),
  constraint fk_approver foreign key (approver) references user_info (user_id),
  constraint fk_creator foreign key (creator) references user_info (user_id),
  constraint fk_factory foreign key (factory) references factorys (id),
  constraint fk_purchase foreign key (purchase_id) references base_purchases (id)
);


------------------------------------------------------------------------------------
-- 生产相关sql version 3.5.0 update 2019/4/15 jcj_version
------------------------------------------------------------------------------------------

-- 总类目池
CREATE TABLE if not exists base_material_category_pool
(
  id        varchar(36) PRIMARY KEY default uuid_generate_v4(), -- 类目id
  name      varchar(50) not null,                               -- 类目名称
  parent_id varchar(36),                                        -- 上一级类目id
  time      integer,                                            -- 创建时间
  constraint fk_parent_id foreign key (parent_id) REFERENCES base_material_category_pool (id)
);

-- 添加“其他”的类目
insert into base_material_category_pool(id, name, time)
values ('07aa367b-d978-4948-ae80-575979f31689', '其他', 1554996996);

-- 总产品/物料池
CREATE TABLE if not exists base_materials_pool
(
  id          varchar(36) PRIMARY KEY,                                    -- 产品或者物料的id
  name        varchar(50) not null,                                       -- 产品或物料名称
  unit        varchar(5)  not null,                                       -- 产品或物料单位
  category_id varchar(36) default '07aa367b-d978-4948-ae80-575979f31689', -- 产品或物料类别
  time        integer,                                                    -- 创建时间
  constraint fk_category foreign key (category_id) REFERENCES base_material_category_pool (id)
);

-- 实际生产单表
CREATE TABLE if not exists base_product_task
(
  id                 varchar(36) PRIMARY KEY,    -- 生产单id
  factory            varchar(36) not null,
  product_id         varchar(36) not null,       -- 产品id
  target_count       float       not null,
  complete_count     float         default 0,
  state              varchar(1)    default '0',  -- 生产单状态，0：待生产，1：待领料，2：生产中，3：已完工
  remark             varchar(50)   default '',
  time               integer     not null,       -- 创建的时间
  plan_complete_time integer,                    -- 计划完成生产的时间
  prepare_time       integer,                    -- 备好料的时间
  start_time         integer,                    -- 开始生产的时间
  complete_time      integer,                    -- 完成生产的时间
  material_ids       varchar(36)[] default '{}', -- 生产单中的物料列表（单个产品）
  material_counts    float[]       default '{}', -- 生产单中的物料数量（单个产品）
  purchase_state     varchar(1)    default '0',  -- 是否创建了采购单，0：未创建， 1：已创建
  order_id           varchar(36) not null,
  constraint fk_factory foreign key (factory) REFERENCES factorys (id),
  constraint fk_order_id foreign key (order_id) REFERENCES base_orders (id),
  constraint fk_product_id foreign key (product_id) REFERENCES base_materials_pool (id)
);
-- 拆成三个单，实际生产单，父单子单关系表，父单表，

-- 父生产单表
CREATE TABLE if not exists base_product_parent_task
(
  id                 varchar(36) PRIMARY KEY,    -- 父生产单id
  factory            varchar(36) not null,       -- factory
  product_id         varchar(36) not null,       -- 产品id
  target_count       float       not null,
  complete_count     float         default 0,
  state              varchar(1)    default '0',  -- 生产单状态，0：待生产，1：待领料，2：生产中，3：已完工
  remark             varchar(50)   default '',
  time               integer     not null,       -- 创建的时间
  plan_complete_time integer,                    -- 计划完成生产的时间
  prepare_time       integer,                    -- 备好料的时间
  start_time         integer,                    -- 开始生产的时间
  complete_time      integer,                    -- 完成生产的时间
  material_ids       varchar(36)[] default '{}', -- 生产单中的物料列表（单个产品）
  material_counts    float[]       default '{}', -- 生产单中的物料数量（单个产品）
  purchase_state     varchar(1)    default '0',  -- 是否创建了采购单，0：未创建， 1：已创建
  order_id           varchar(36) not null,
  constraint fk_factory foreign key (factory) REFERENCES factorys (id),
  constraint fk_order_id foreign key (order_id) REFERENCES base_orders (id),
  constraint fk_product_id foreign key (product_id) REFERENCES base_materials_pool (id)
);

-- 父子单关系表
CREATE TABLE if not exists base_product_relation
(
  child_id  varchar(36) PRIMARY KEY, -- 子单id
  parent_id varchar(36) not null,    -- 父单id
  time      integer
);

-- 工厂-产品表
CREATE TABLE if not exists base_products
(
  id               varchar(36) not null,
  price            double precision,
  factory          varchar(36),
  loss_coefficient float default 0,
  time             integer,
  constraint base_products_pkey PRIMARY KEY (id, factory),
  constraint fk_id foreign key (id) REFERENCES base_materials_pool (id),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id)
);

-- 工厂-物料表
CREATE TABLE if not exists base_materials
(
  id               varchar(36) not null,
  price            double precision,
  lowest_count     double precision, -- 最低采购量
  factory          varchar(36),
  loss_coefficient float default 0,
  time             integer,
  constraint base_materials_pkey PRIMARY KEY (id, factory),
  constraint fk_id foreign key (id) REFERENCES base_materials_pool (id),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id)
);

-- 工厂-工序表
CREATE TABLE if not exists base_processes
(
  id      varchar(36) PRIMARY KEY,
  name    varchar(50) not null,
  factory varchar(36) not null,
  del     varchar(1) default '0', -- 该工序是否已删除
  time    integer,
  constraint fk_factory foreign key (factory) REFERENCES factorys (id)
);

-- 产品工序表
CREATE TABLE if not exists base_product_processes
(
  product_id      varchar(36) not null,       -- 产品id
  factory         varchar(36) not null,
  process_step    varchar(2)  not null,       -- 工序序号
  process_id      varchar(36) not null,       -- 工序id
  material_ids    varchar(36)[] default '{}', -- 工序中的物料列表
  material_counts float[]       default '{}', -- 工序中的物料数量
  --  del                   varchar(1) default '0',     -- 该产品工序是否已删除
  time            integer,
  constraint base_product_processes_pkey PRIMARY KEY (factory, product_id, process_step),
  constraint fk_product_id foreign key (product_id) REFERENCES base_materials_pool (id),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id)
);

-- 生产任务进度表
CREATE TABLE if not exists base_product_task_processes
(
  id              serial primary key,
  product_task_id varchar(36) NOT NULL, -- 生产单id
  process_step    varchar(36) NOT NULL, -- 工序序号
  start_time      integer,
  end_time        integer,
  take_time       double precision,
  good            integer,              -- 良品数量
  ng              integer,              -- 劣品数量
  remark          varchar(60),
  time            integer,
  creator         varchar(36),
  factory         varchar(36) not null,
  constraint fk_product_task_id foreign key (product_task_id) REFERENCES base_product_task (id),
  constraint fk_creator foreign key (creator) REFERENCES user_info (user_id),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id)
);

-- 退料单
CREATE TABLE if not exists base_material_return
(
  id              varchar(36) PRIMARY KEY,  -- 退料单id
  state           varchar(1)   default '0', -- 退料状态，0：未退料，1：已退料
  material_ids    varchar(36)[],            -- 物料列表
  material_counts double precision[],       -- 物料数量
  creator         varchar(36) not null,     -- 创建者
  receiver        varchar(36),              -- 签收者
  create_time     integer,                  -- 创建的时间
  finish_time     integer,                  -- 退料的时间
  factory         varchar(36) not null,
  remark          varchar(100) default '',  -- 退料原因
  product_task_id varchar(36),              -- 生产任务单id
  constraint fk_creator foreign key (creator) REFERENCES user_info (user_id),
  constraint fk_receiver foreign key (receiver) REFERENCES user_info (user_id),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id),
  constraint fk_product_task_id foreign key (product_task_id) REFERENCES base_product_task (id)
);

-- 补料单
-- 这里没有给每个状态设置时间，是因为和领料单完全一样，没必要再重复
CREATE TABLE if not exists base_material_supplement
(
  id              varchar(36) PRIMARY KEY,  -- 补料单id
  state           varchar(1)   default '0', -- 补料状态，0：未备料，1：待领料，2：已领料
  material_ids    varchar(36)[],            -- 物料列表
  material_counts double precision[],       -- 物料数量
  creator         varchar(36) not null,     -- 创建者
  create_time     integer,                  -- 创建的时间
  factory         varchar(36) not null,
  remark          varchar(100) default '',  -- 补料原因
  product_task_id varchar(36),              -- 生产任务单id
  constraint fk_creator foreign key (creator) REFERENCES user_info (user_id),
  constraint fk_factory foreign key (factory) REFERENCES factorys (id),
  constraint fk_product_task_id foreign key (product_task_id) REFERENCES base_product_task (id)
);

------------------------------------------------------------------------------------------------------------------------

-- 仓库-完工入库单
create table base_store_completed_storage
(
  "id"              varchar(36) primary key,                                                                    -- 单号
  "order_id"        varchar(36) not null references base_orders (id) on delete cascade on update cascade,       -- 关联的订单id
  "product_task_id" varchar(36) not null references base_product_task (id) on delete cascade on update cascade, -- 关联的生产任务单id
  "factory"         varchar(36) not null,
  "state"           varchar(1)  not null default '0',                                                           -- 完工入库单状态 0: 未入库，1: 已入库
  "time"            integer     not null,                                                                       -- 创建完工入库单时间
  "completed_time"  integer,                                                                                    -- 入库时间
  "send_person"     varchar(36) references user_info (user_id) on delete cascade on update cascade,             -- 交接人user_id
  "receive_person"  varchar(36) references user_info (user_id) on delete cascade on update cascade              -- 接收人user_id
);

-- 仓库-发货单(订单生成/完工入库单生成)
create table base_store_invoice
(
  "id"                   varchar(36) primary key,                                                        -- 单号
  "order_id"             varchar(36) references base_orders (id) on delete cascade on update cascade,    -- 关联的订单id
  "completed_storage_id" varchar(36),                                                                    -- 关联的完工入库单id
  "factory"              varchar(36) not null,
  "state"                varchar(1)  not null default '0',                                               -- 发货单状态 0: 待发货, 1: 已发货, 2: 已送达, 3: 已取消
  "time"                 integer     not null,                                                           -- 发货单创建时间
  "deliver_person"       varchar(36) references user_info (user_id) on delete cascade on update cascade, -- 发货人
  "deliver_time"         integer                                                                         -- 发货时间
);

-- 仓库-采购入库单
create table base_store_purchase_warehousing
(
  "id"            varchar(36) primary key,                                                                     -- 单号
  "order_id"      varchar(36) not null references base_orders (id) on delete cascade on update cascade,        -- 关联的订单id
  "invoice_id"    varchar(36) not null references base_store_invoice (id) on delete cascade on update cascade, -- 关联的发货单id
  "factory"       varchar(36) not null,
  "state"         varchar(1)  not null default '0',                                                            -- 采购入库状态，0: 未入库，1: 已入库
  "time"          integer     not null,                                                                        -- 创建采购入库单时间
  "income_person" varchar(36) references user_info (user_id) on delete cascade on update cascade,              -- 入库人
  "income_time"   integer                                                                                      -- 入库时间
);

-- 仓库-领料单
create table base_store_picking_list
(
  "id"              varchar(36) primary key,                                                                    -- 单号
  "order_id"        varchar(36) not null references base_orders (id) on delete cascade on update cascade,       -- 关联的订单id
  "product_task_id" varchar(36) not null references base_product_task (id) on delete cascade on update cascade, -- 关联的生产任务单id
  "supplement_id"   varchar(36),                                                                                -- 关联base_material_supplement(id)的补料单id
  "factory"         varchar(36) not null,
  "state"           varchar(1)  not null default '0',                                                           -- 领料单状态，0: 待备料，1: 待领料，2: 已领料
  "style"           varchar(1)  not null default '0',                                                           -- 领料单类型，0: 生产单直接创建，1:补料单创建
  "time"            integer     not null,                                                                       -- 创建领料单时间
  "waited_time"     integer,                                                                                    -- 完成备料后代领料时间
  "picking_time"    integer,                                                                                    -- 已领料时间
  "send_person"     varchar(36) references user_info (user_id) on delete cascade on update cascade,             -- 发料人user_id
  "receive_person"  varchar(36) references user_info (user_id) on delete cascade on update cascade              -- 领料人user_id
);

--仓库-物料库存操作记录
create table base_materials_log
(
  "id"            serial primary key,
  "material_id"   varchar(36) not null references base_materials_pool (id) on delete cascade on update cascade, -- 关联物料池的id
  "type"          varchar(20),                                                                                  -- actual/on_road/prepared/store_check
  "count"         double precision,
  "source"        varchar(1)  not null,                                                                         -- 哪种类型的单操作的库存: 0: 入库-采购入库单, 1: 出库-领料单, 2: 库存盘点, 3: 生产单, 4：退料单
  "source_id"     varchar(36) not null,                                                                         -- 单号id
  "factory"       varchar(36) not null,
  "product_count" double precision,                                                                             -- 生产任务单数量
  "time"          integer
);

-- 仓库-物料库存
create table base_materials_storage
(
  "material_id" varchar(36) references base_materials_pool (id) on update cascade on delete cascade, -- 物料id
  "factory"     varchar(36) not null,
  "actual"      double precision,                                                                    -- 实际库存
  "on_road"     double precision,                                                                    -- 在途库存
  "prepared"    double precision,                                                                    -- 预分配库存
  "safety"      double precision,                                                                    -- 安全库存
  "time"        integer,
  constraint base_materials_storage_pkey PRIMARY KEY (material_id, factory)
);

--仓库-产品库存操作记录
create table base_products_log
(
  "id"         serial primary key,
  "product_id" varchar(36) not null references base_materials_pool (id) on delete cascade on update cascade, -- 关联的物料池的id
  "type"       varchar(20),                                                                                  -- actual/pre_product/prepared/store_check
  "count"      double precision,
  "source"     varchar(1)  not null,                                                                         -- 哪种类型的单操作的库存: 0: 入库-完工入库单, 1: 出库-发货单, 2: 库存盘点, 3：生产单, 4: 订单
  "source_id"  varchar(36) not null,                                                                         -- 单号id
  "factory"    varchar(36) not null,
  "time"       integer
);

-- 仓库-产品库存
create table base_products_storage
(
  "product_id"  varchar(36) references base_materials_pool (id) on update cascade on delete cascade, -- 产品id
  "factory"     varchar(36) not null,
  "actual"      double precision,                                                                    -- 实际库存
  "pre_product" double precision,                                                                    -- 预生产库存
  "prepared"    double precision,                                                                    -- 预分配库存
  "safety"      double precision,                                                                    -- 安全库存
  "time"        integer,
  constraint base_products_storage_pkey PRIMARY KEY (product_id, factory)
);

-- 仓库-库存盘点单
create table base_storage_check
(
  "id"            varchar(36) primary key,                                                             -- 单号
  "factory"       varchar(36),
  "material_id"   varchar(36) references base_materials_pool (id) on update cascade on delete cascade, -- 物料id
  "type"          varchar(10) not null,                                                                -- material/product
  "before"        double precision,                                                                    -- 盘点前数量
  "after"         double precision,                                                                    -- 盘点后数量
  "more_less"     varchar(1),                                                                          -- 盘盈: 0, 盘亏: 1
  "state"         varchar(1)  not null default '0',                                                    -- 0: 待审批，1: 审批通过(已审批)，2: 审批未通过(已审批)
  "remark"        varchar(60),
  "creator"       varchar(36) references user_info (user_id) on update cascade on delete cascade,      -- 盘点人
  "time"          integer,                                                                             -- 创建时间
  "approval"      varchar(36) references user_info (user_id) on update cascade on delete cascade,      -- 审批人
  "approval_time" integer                                                                              -- 审批时间
);

-- 仓库-临时申购单
create table base_store_temporary_purchase
(
  "id"            varchar(36) primary key,                                                        -- 单号
  "factory"       varchar(36) not null,
  "purchase_id"   varchar(36),                                                                    -- 创建的采购单id
  "state"         varchar(1)  not null default '0',                                               -- 0: 待审批，1: 审批通过, 2: 审批不通过, 3: 已取消
  "remark"        varchar(60)          default '',
  "creator"       varchar(36) references user_info (user_id) on update cascade on delete cascade, -- 创建人
  "time"          integer,                                                                        -- 创建时间
  "approval"      varchar(36) references user_info (user_id) on update cascade on delete cascade, -- 审批人
  "approval_time" integer                                                                         -- 审批时间
);

-- 仓库-临时申购单对应物料
create table base_store_temporary_purchase_materials
(
  "id"          serial primary key,
  "purchase_id" varchar(36) references base_store_temporary_purchase (id) on update cascade on delete cascade, -- 关联的临时申购单id
  "material_id" varchar(36),                                                                                   -- 关联base_materials物料id
  "count"       double precision                                                                               -- 申购数量
);


------------------------------------------------------------------------------------
-- 订单，采购单相关sql (Part2) version 3.5.0 update 2019/04/23
------------------------------------------------------------------------------------------


-- 采购单对应物料表
create table base_purchase_materials
(
  id            serial primary key,
  purchase_id   varchar(36) null,
  product_id    varchar(36) null, -- 物料id, 为了方便代码统一先取名叫product_id
  product_count float8      null,
  unit_price    float8      null,
  constraint fk_purchase foreign key (purchase_id) references base_purchases (id),
  constraint fk_product foreign key (product_id) references base_materials_pool (id)
);

create table base_order_products
(
  id            serial primary key,
  order_id      varchar(36) null, -- 订单id
  product_id    varchar(36) null, -- 产品id
  product_count float,            -- 产品
  unit_price    float,            -- 产品单价
  constraint fk_order foreign key (order_id) references base_orders (id),
  constraint fk_product foreign key (product_id) references base_materials_pool (id)
);

create table base_order_track
(
  id       varchar(36) primary key default uuid_generate_v4(),
  order_id varchar(36) references base_orders (id), -- 订单id
  type     varchar(1) not null,                     -- 类型，1: 状态变换，2：收款
  val      float      not null,                     -- 状态变换时，为状态类型， 收款时为收款金额
  time     integer                 default extract(epoch from now())::integer
);


create table base_supplier_materials
(
  id          serial primary key,
  factory_id  varchar(36) REFERENCES factorys (id),
  supplier_id varchar(36) REFERENCES base_clients_pool (id),
  material_id varchar(36) REFERENCES base_materials_pool (id),
  unit_price  float,
  reate_time  int4 default extract(epoch from now())::integer
);

create table base_client_products
(
  id          serial primary key,
  factory_id  varchar(36) REFERENCES factorys (id),
  client_id   varchar(36) REFERENCES base_clients_pool (id),
  product_id  varchar(36) REFERENCES base_materials_pool (id),
  unit_price  float,
  create_time int4 default extract(epoch from now())::integer
);


ALTER TABLE factorys
  ADD COLUMN seq_id SERIAL;
alter table base_clients_pool add column verify varchar(1) default '1'; -- verify  添加到资源池企业需经审核 0:未审核，1:已审核

------------------------------------------------------------------------------------------------------------------------
-- version 3.5.1 update 2019.5.14
------------------------------------------------------------------------------------------------------------------------
alter table base_store_invoice
  add column remark varchar(60) default ''; -- 发货单备注
alter table base_storage_check
  add column reason varchar(60) default ''; -- 盘点原因
alter table base_store_temporary_purchase
  add column reason varchar(60) default ''; -- 临时申购原因

-- 仓库-多仓库总表
create table base_multi_storage
(
  "id"      serial primary key,
  "uuid"    varchar(36),
  "name"    varchar(30) not null, -- 仓库名称
  "factory" varchar(36) not null references factorys (id) on update cascade on delete cascade,
  "time"    integer
);

alter table base_materials_storage
  add column uuid varchar(36) default 'default';

alter table base_products_storage
  add column uuid varchar(36) default 'default';

-- 产品表增加
alter table base_products
  add column lowest_count double precision default 0,     --最低采购量
  add column lowest_package double precision default 0,   --最小包装量
  add column lowest_product double precision default 0;   --最小生产量

-- 物料表删除最低采购量
alter table base_materials
  drop column lowest_count;

-- 产品工序表增加单位用时
alter table base_product_processes
  add column unit_time double precision;

-- 修改good和ng的数据类型
alter table base_product_task_processes alter column good type double precision, alter column ng type double precision;

-- 增加客户的送达时间
alter table base_clients add column deliver_days float default 0;
alter table base_suppliers add column deliver_days float default 0;
alter table base_supplier_materials
  add column lowest_package float default 0,  -- 最小包装量
  add column lowest_count float default 0;  -- 最小起订量

-- 订单状态更新记录日志
create table base_orders_stats (
	id serial primary key,
	order_id varchar(36) references base_orders(id),
	state varchar(1) not null,								-- 1:创建，2: 审批，3:内部取消，4: 暂停，5：外部取消 6: 暂停恢复
	remark varchar(60) default '',							-- 操作备注
	operator varchar(36) references user_info(user_id),		-- 操作人
	optime integer,											-- 操作时间
	time integer default extract(epoch from now())::integer
);

alter table base_purchases add column canceler varchar(36) references user_info(user_id);
alter table base_purchases add column cancel_remark varchar(60) default '';
alter table base_purchases add column arrival_time integer default 0;