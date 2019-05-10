相关模块发送RabbitMQ消息

| Module | API | Method | Resource Type | Data | Explain |  
|:----:|:----:|:------:|:----------:|:-----|:-----|  
|订单| /order/{Id}| PUT| PyOrderState | {"fac": "string", "id": "string", "state": "{STATE}"}| STATE: 1: 订单审批通过， 2：订单已取消, 3:订单已发货 4: 订单已暂停，5：订单已启动, 6: 待审批订单， 7: 订单已送达|  
|采购 | /purchase/{Id}| PUT| PyPurchaseState | {"fac": "string", "id": "string", "state": "{STATE}"}| STATE: 1: 新增采购单， 2：采购中, 3:运输中 4: 已入库，5：已取消|
|仓库|store/invoice/detail|POST|PyInvoice|{"fac": "string", "id": "string", "state": "{STATE}"}|STATE: 1: 新增发货单, 2: 发货单已送达
|仓库| |POST|PyPickingList|{"fac": "string", "id": "string", "state": "{STATE}"}|STATE: 1: 新增领料单, 2: 待领料
|仓库| |POST|PyCompletedStorage|{"fac": "string", "id": "string", "state": "{STATE}"}|STATE: 1: 新增完工入库单, 2: 完工入库单已入库
|仓库| |POST|PyPurchaseWarehousing|{"fac": "string", "id": "string", "state": "{STATE}"}|STATE: 1: 新增采购入库单
|仓库| |POST|PyStoreCheck|{"fac": "string", "id": "string", "state": "{STATE}"}|STATE: 1: 新增库存盘点单, 2: 库存盘点通过, 3: 库存盘点未通过
|仓库| |POST|PyTemporaryPurchase|{"fac": "string", "id": "string", "state": "{STATE}"}|STATE: 1: 新增临时申购单, 2: 临时申购通过, 3: 临时申购未通过
|生产 | | | PyProductTaskCreate | {"fac": "string", "id": "string", "state": "{STATE}"} | 1: 新增生产任务, 2：待领料, 3：生产中|  
|生产 | /product/task/done/{id}| POST| PyProductTaskDoneId | {"fac": "string", "id": "string", "user_id": "string", "state": "{STATE}"} | 1: 已完工-未入库, 2: 已完工-已入库|  
|生产 | /product/material/return/create| POST| PyProductMaterialReturnCreate |{"fac": "string", "id": "string", "user_id": "string", "state": "{STATE}"}|1：新增退料单, 2：已退料 |  
|生产 | /product/material/supplement/create| POST| PyProductMaterialSupplementCreate |{"fac": "string", "id": "string", "user_id": "string", "state": "1"}|1：新增补料单|  
|生产 | | | PyProductMaterialSupplementUpdate |{"fac": "string", "id": "string", "state": "{STATE}"}| 1：补料-待领料，2：补料-已领料|  