# DEM 拼接服务(FastAPI + GDAL)

把相邻架次的高程栅格拼成统一底图:网页上传最多 **4 份带坐标系的单波段 GeoTIFF**,
调整覆盖顺序、设置目标像元边长(米,>0),后台统一重投影到 **EPSG:3857** 后镶嵌,
再转换为**带概览的 COG**。全部成功才开放下载,并显示范围与尺寸。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`samplegen` 服务会先生成演示样例(写入 `./data/samples`),完成后 `web` 自动启动。
浏览器中可直接勾选样例、调整顺序、提交拼接、取消任务、下载结果,无需准备数据。

> 若无法拉取 `ghcr.io/osgeo/gdal`,把 Dockerfile 第一行改为
> `osgeo/gdal:ubuntu-small-3.9.2`(Docker Hub)。

## 演示样例(不同坐标系 / 重叠 / NoData)

| 文件 | CRS | 说明 |
|---|---|---|
| `dem_A_utm32n.tif` | EPSG:32632 | 2m,中部一块 NoData 空洞 |
| `dem_B_utm32n.tif` | EPSG:32632 | 与 A 重叠约 200m×500m,西侧 NoData 条带 |
| `dem_C_wgs84.tif` | EPSG:4326 | 经纬度(度),覆盖 A 西南角,含 NoData 空洞 |
| `dem_D_webmerc.tif` | EPSG:3857 | 与 B 东北角重叠,四周一圈 NoData 边缘 |

建议体验顺序:勾选 A、B、C、D 加入列表 → 像元边长 `2` → 开始拼接。
把 B 调到 A 之下/之上各跑一次,可看到**列表靠后的文件有效像元优先**,
而 NoData 不会盖掉已有高程。重新生成样例:`FORCE=1 docker compose run --rm samplegen`。

## 处理流水线

每个任务三个 GDAL 阶段,全部流式处理,**不把整幅栅格读入内存**:

1. **MOSAIC** — `gdalwarp -t_srs EPSG:3857 -tr <res> <res> -r bilinear -dstnodata -9999`
   多输入一次镶嵌:自动计算并集范围;输入按列表顺序依次写入,**后文件有效像元覆盖先文件**;
   各输入自带的 NoData 被识别为掩膜,不参与写入,因此不会盖掉已有高程。
2. **COG** — `gdal_translate -of COG -co COMPRESS=DEFLATE -co RESAMPLING=AVERAGE`,
   COG 驱动自动生成概览。
3. **METADATA** — `gdalinfo -json` 读取尺寸与地理变换,计算 EPSG:3857 范围(米)。

只有三个阶段全部成功,任务才进入 `DONE` 并开放下载。

## 并发、取消与资源回收

- 全局信号量限制 **2 个任务并行**,其余排队(`QUEUED`);信号量在**整个收尾完成后**
  才释放——任务状态变为终态时,进程回收与暂存清理必然已经结束,名额才归还。
- 每个 GDAL 阶段都是独立子进程,通过 `start_new_session` 成为独立会话/进程组首进程,
  其派生的同组工作进程(如 `gdalwarp -multi`)pgid 相同。取消或失败时对该进程组发
  `SIGKILL`,只影响本任务,不影响另一并行任务。
- **以主进程退出(`returncode`)为准,不以日志管道 EOF 为准**。这是本次修复的核心:
  GDAL 主进程异常退出后,同组子进程可能
  (1)仍持有 stdout 管道——EOF 永不到来,旧实现里 `await proc.wait()` 被管道拖住,
  取消后任务一直显示"运行中";
  (2)已关闭日志管道继续干活——EOF 提前到来,旧实现据此提前判定失败,并在子进程
  仍在写目录时删除暂存。
  调度器轮询由 SIGCHLD 置位的 `returncode`(与管道无关),主进程一退出立即按进程组
  SIGKILL 残留,并轮询 `/proc` 确认整组消失后才进入下一步,绝不等待日志自行关闭。
- **统一、幂等的收尾路径**:停止同组进程 → 等待回收 → 排空/关闭日志管道 → 清理暂存
  → 置终态 → 唤醒等待者。实际的停止回收每个任务只执行一次(共享 Future),取消端点
  与运行协程复用同一结果;重复取消等待同一个收尾 Future,不会触发第二次收尾。
- **阶段交接安全**:阶段启动前、主进程退出后、发布结果前都检查取消标志——取消与主进程
  退出交错时绝不启动下一阶段,也绝不发布迟到结果或开放下载。
- 失败或取消后,先终止并回收本任务全部同组进程,再删除该任务暂存目录;
  成功任务保留 `result.tif` 供下载。服务重启时清空遗留暂存。

## 故障演示(主进程先退 · 日志管道持有/关闭)

网页"3. 目标像元边长并提交"中的 **MOSAIC 故障演示** 下拉框提供三种模式
(也可在创建任务的表单里传 `demo_mode`):

| 模式 | 行为 | 旧实现的问题 | 修复后 |
|---|---|---|---|
| `off` | 真实 `gdalwarp`,正常拼接 | — | 正常 `DONE` 并可下载 |
| `hold` | 主进程工作约 8 秒后异常退出,同组子进程**继续持有日志管道** | 管道不 EOF,任务永远显示运行,取消也无法终态 | 检测到主进程退出即整组 SIGKILL,确认回收+清暂存后报 `FAILED` |
| `close` | 主进程先退,同组子进程**关闭日志管道但继续干活** | 日志提前 EOF → 提前报失败,并在子进程仍工作时删暂存 | 不据 EOF 收尾,先终止回收子进程,再清暂存报 `FAILED` |

演示桩 `app/demo/gdalwarp_demo.py` 用 `fork` 复刻 GDAL 的同组多进程结构,只替换
MOSAIC 阶段的可执行程序,输出文件名/工作目录与真实流水线一致,后续 COG/METADATA
规则不变。任务卡片会显示"收尾确认:同组残留进程已终止回收 N 个"。

手动核对(容器内):

```bash
docker compose up --build -d
# 网页选 hold 或 close 提交一个任务(也可在主进程存活的约 8 秒内点取消,验证交错收尾)
docker compose exec web ps -eo pid,pgid,stat,cmd | grep -E 'gdalwarp_demo|PID'
# 任务进入 FAILED/CANCELLED 后:无残留 demo 进程,data/tasks 下对应目录已删除
ls data/tasks/
# 再提交一个 off 正常任务,确认两任务并行互不影响、成功后可下载
```

可用环境变量调整演示时序:`DEMO_PARENT_SLEEP`(主进程退出前秒数,默认 8)、
`DEMO_CHILD_RUN`(子进程残留秒数,默认 300)。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 网页 UI |
| GET | `/api/samples` | 样例列表 |
| POST | `/api/tasks` | 创建任务(multipart:`resolution`、`plan` JSON、可选 `demo_mode`、`files`) |
| GET | `/api/tasks` | 任务列表(状态/阶段/结果/日志尾/收尾确认) |
| POST | `/api/tasks/{id}/cancel` | 取消任务(等待同次收尾完成后返回终态) |
| GET | `/api/tasks/{id}/download` | 下载 COG(仅 `DONE`,否则 409) |

`plan` 示例(按覆盖优先级从低到高):
`[{"type":"upload","index":0},{"type":"sample","name":"dem_B_utm32n.tif"}]`

## 目录结构

```
app/main.py          FastAPI 路由(上传校验、任务 API、下载)
app/taskmanager.py   任务调度:并发限制、三阶段流水线、主进程退出检测、取消与清理(不依赖 FastAPI)
app/demo/gdalwarp_demo.py  MOSAIC 故障演示桩(主进程先退,子进程持有/关闭日志管道)
app/static/index.html 单页 UI
samples/generate_samples.py 样例生成(标准库 + gdal_translate)
tests/test_taskmanager.py  调度逻辑测试(GDAL 桩程序,无需真实 GDAL)
data/                运行期数据(样例 + 每任务暂存目录,Compose 自动创建)
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data` | 数据根目录 |
| `SAMPLES_DIR` | `$DATA_DIR/samples` | 样例目录 |
| `MAX_PARALLEL` | `2` | 并行任务数 |

注意:任务注册表在进程内,uvicorn 必须单 worker(镜像默认如此)。

## 验证取消时的进程回收(容器内)

```bash
docker compose up --build -d
# 网页提交一个大任务后立即取消,然后:
docker compose exec web ps aux   # 不应残留 gdalwarp/gdal_translate 进程
ls data/tasks/                   # 被取消任务的目录已清理
```

## 本地运行调度测试(无需 GDAL)

```bash
python3 -I tests/test_taskmanager.py
```

覆盖:两任务并行与排队、取消运行中任务(killpg 参数与清理)、取消排队任务、
COG 阶段取消不发布结果、重复取消只做一次收尾(共享 Future)、槽位在收尾后释放,
以及**主进程异常先退的两种管道局面**(孙进程持有日志 / 关闭日志继续干活):
调度器均须在孙进程仍存活时保持 RUNNING、不删暂存,主动终止整组并确认回收后才报
FAILED。沙箱环境禁止真实信号时,测试用哨兵文件模拟进程组终止;真实 `SIGKILL`
在 Docker 容器中生效(README 的手动核对步骤与仓库外的端到端真实信号测试覆盖)。
