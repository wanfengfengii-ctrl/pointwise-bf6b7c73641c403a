# 压缩导出断点入库契约

这是已有的监管资料RPA导出采集器。本轮只修复接收、解码、校验、发布、恢复这一条链路。portal.py模拟录制的真实远端协议，不能修改其确认后删除载荷、请求绑定和重投语义来规避故障；samples.py及checks.py原断言须保留。允许修改内部辅助接口、增加本地表和索引，不引入外部服务。

## 调用、输入与输出

公共接口为Collector(db, portal, job)、step(fail_at=None, lose_ack=False)、read()、status()，以及Portal的既有方法。db可以是SQLite路径或sqlite3.Connection；路径不存在时自动建库，旧库可继续打开，不得清库。每个job同时只有一个采集worker；不同job可以顺序或交替执行，不要求实现租约调度。

step最多处理一个远端响应：完成本次处理返回True，已完成任务返回False。异常仍向调用者抛出。每个响应含从0连续递增的seq、最多65536字节body及仅末段为真的final；重试未确认响应保持内容不变。ack按job、seq和SHA-256绑定，可幂等重试；确认提交后该段不可再fetch，不能读取远端数据库内部来取回它。lose_ack=True令远端成功确认后抛LostReply。远端等待时不得持本地写事务。

run.py提供磁盘入口，argv为python3 run.py install|step|read|status LOCAL_DB PORTAL_DB CASE。CASE取samples.py中的normal、restart、corrupt、conflict、split_members；install只安装录制源，step每次调用均重建Collector，重复调用直至finished=true。成功stdout为一个JSON值；失败输出{"error":"异常文本"}并退出2。install返回{"job":CASE,"installed":true}；step返回{"advanced":布尔,"status":状态对象,"records":记录列表}；read返回记录列表；status返回{"local":状态对象,"portal":远端统计}。文件参数示例：python3 run.py install local.db portal.db normal，随后反复python3 run.py step local.db portal.db normal，最后python3 run.py read local.db portal.db normal。

read按id排序返回未删除的[id,rev,station,reading]列表，空时为[]。status返回{"job":job,"next_seq":整数,"finished":布尔,"error":字符串或null}；next_seq为本地已完成处理的完整响应数，不等于已远端确认数。finished只在流、字符及记录全部合法结束时为true；失败任务须持久保留非空error，read仍能读取此前有效成员的记录，后续step拒绝继续。error文本不固定，必须能区分压缩损坏、UTF-8/CSV错误和修订冲突，不要求新增报告格式。

源是一个或多个拼接的gzip成员，解压后拼成单一UTF-8 CSV流，首字符允许BOM。只在流开头有一份固定表头id,rev,station,reading,deleted。LF或CRLF换行，末记录必须有行终止符；双引号包裹字段，双双引号转义，包裹字段可含逗号和换行。每字段解码后UTF-8长度不超过65536字节；id、station非空，rev最多19字符且为1到9223372036854775807的十进制数字，reading最多20字符且为有符号64位十进制整数，deleted只能为0或1。沿用csvstream.py的其余字段校验；不增加字段或容错替换。空数据流用仅含表头的合法gzip表示。

## 唯一正确性边界

响应边界、gzip成员边界、UTF-8字符边界、CSV字段和记录边界可以任意错开，一个响应可含多个成员。成员允许为空，gzip头允许标准可选字段；校验头、解压、CRC和长度不得省略。final后有未闭合gzip、UTF-8或CSV即报错；非gzip尾随字节也报错。跨成员记录归属于补齐其行终止符的成员，CRLF的CR已补齐记录而LF只被吞掉。

成员校验通过后才可发布由其补齐的完整记录；成员中的任一压缩、字符、CSV或修订错误使该成员所有记录不可见，但同一响应中此前有效成员必须保留。最后成员还必须通过流末尾的UTF-8及CSV闭合校验。跨成员未完成的字符与记录继续携带；不能因成员结束而补换行、替换字符或重复表头。

每条记录按文件行序相对当前已发布记录和本成员前序行判定：低于当前最高rev忽略，同版同内容忽略，当前最高同版异内容使整成员失败，更高版完整替换。deleted=1仍保存修订和全部内容，只从read隐藏；高版可复活。失败成员不能泄漏它的早期行、墓碑、最高修订或解析检查点。

每次对外可见状态都必须是按成员顺序提交的完整有效前缀。本地持久化中断、确认响应丢失和两个数据库重开不得漏段、重复效果或发布半成员。after_store、after_ack、after_apply、after_commit仍在对应已有阶段抛Injected；after_apply应处于本次最终本地处理事务提交前，after_commit在其提交后，早先已经发布的独立有效成员不回滚。注入后调用者丢弃worker，重新打开连接和Collector恢复。新增持久化边界同样允许进程终止。

## 留存与空间

step额外Python峰值分配不得超过32MiB，不计调用方保存的录制包和read结果；不能随成员解压长度、任务长度或总记录数线性增长。可以用SQLite暂存未验证记录和原始分段；不能整成员解压入内存。远端不可重取，重开允许从本地重放当前未完成成员，不能从任务开头重放、重复解析已持久发布成员或永久保留整任务历史。

完成一次step后，原始压缩载荷的逻辑留存量不得超过当前未验证成员已接收的压缩字节数加65536字节；在有效边界及时删除不再用于恢复的载荷和暂存行。完成或确认失败后只留业务表、恢复元数据及有界诊断，不保留已消费载荷和失败成员暂存行；SQLite空闲页不计逻辑留存量。上述限制不禁止保存最终每id最高修订记录。不能跨远端调用持有本地写事务，也不能通过推迟远端确认到整任务结束绕开断点恢复。

## 已有回归和事故

python3 checks.py normal须退出0并输出NORMAL_REGRESSION_OK。
python3 checks.py repair当前退出1并输出CSV_RESTART_INTEGRITY_MISMATCH，修复后须退出0并输出REPAIR_REGRESSION_OK。
normal覆盖最高修订、墓碑、完成任务重开，以及UTF-8和CSV跨gzip成员；repair包含逐响应重开、确认响应丢失、四个故障点、坏CRC提前可见、同一响应的好坏成员边界。
normal最终read必须为[["a",2,"east",12]]；restart最终须保留18条north,水源加换行well的记录，完整期望在samples.py。
corrupt只可保留[["good",1,"east",7]]，不能出现poison；conflict同样只可保留good的rev=1，不能留下temp或删除好成员。

checks.py使用SQLite backup替换两个内存数据库连接，销毁所有进程内解析对象；这不是磁盘持久性证明，交付验证还须用run.py磁盘入口及真实进程中断核对恢复结果。
保留.pointwise-runtime.json，verify执行python3 checks.py verify并核对业务，demo执行python3 checks.py demo展示真实记录、本地状态和远端确认统计。交付修复代码与恢复验证说明，不能删减原断言或以只改测试代替修复。
