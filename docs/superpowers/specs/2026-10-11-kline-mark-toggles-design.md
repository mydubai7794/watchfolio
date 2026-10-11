# K线标注线开关设计（kline mark toggles）

日期：2026-10-11
状态：用户已批准交互设计（对话内逐点拍板）

## 1. 背景

K线弹层主图右侧标注虚线 5 组全叠加（成本/止损/22日高/加仓档位 ±X%/Fib 38.2-61.8，
`klDrawMain` 内 `lines` 数组，portfolio.html 约 2384-2395 行），线多时重叠看不清。
用户拍板改为按组开关、选中才展示；成本线移出图面改为悬浮浮层展示。

## 2. 关键决定（用户逐点确认）

1. **三组独立 toggle**：`档位`（marks.levels，±X% 加仓档位线，可多条）、
   `Fib`（Fib38.2/Fib50/Fib61.8 三档回撤线）、`止损/22日高`（marks.stop_level +
   marks.hhv22 两条风控线合并一组）。默认**全关**，选中才画。
2. **localStorage 持久化**：key `klMarkToggles`，值 JSON `{levels,fib,guard}`（布尔），
   跨会话记忆；读取 try/catch + 形状校验（非三布尔对象视为损坏回退全关）。
3. **成本线移图**：`marks.cost` 不再画虚线；改为悬浮浮层 klTip 加一行
   `成本 xxx`（`d.marks.cost` 非空才显示）。
4. **买卖点常驻**：▲▼ 标记不受开关影响（现状保持）。
5. **y 轴范围只算可见线**：`lo/hi` 拉伸（现 2396 行）只纳入当前点亮的组的线——
   隐藏组不得撑开图面留白。
6. **零请求切换**：开关点击 = 翻转 `KL.toggles` + 写 localStorage +
   `klRender()` 重绘（数据已在 `KL.data[KL.tf]`，不发任何请求）。
7. **按钮 UI** 照 `.kline-intabs` 按钮组样式（`.on` 选中态，CSS 294-305 行既有），
   按钮行放主图 canvas（#klMain）上方。
8. **周/月/分时周期**：响应本就无 marks/overlays，开关点亮无线可画（无报错无副作用）。
9. **MA50/MA200 均线不动**；后端 API 零改动。

## 3. 实现（portfolio.html 单文件，三处）

### 3.1 HTML

`<canvas id="klMain">` 之前插入按钮行：

```html
<div class="kline-intabs kline-marks" id="klMarkToggles">
  <button data-mk="levels">档位</button>
  <button data-mk="fib">Fib</button>
  <button data-mk="guard">止损/22日高</button>
</div>
```

（复用 `.kline-intabs` flex+按钮样式，`.kline-marks` 仅补间距微调。）

### 3.2 CSS

不新增规则——按钮行复用 `.kline-intabs` 类（flex 布局/按钮样式/`.on` 选中态/
`margin: 6px 0 2px` 均由 294-305 行既有规则提供）；`kline-marks` 类仅作语义锚点。

### 3.3 JS

- **状态**：`KL` 对象加 `toggles: {levels:false, fib:false, guard:false}`；
  初始化函数 `klLoadToggles()` 读 localStorage（try/catch + 形状校验）后同步三个按钮
  `class="on"`。页面加载时执行一次。
- **事件**：`#klMarkToggles` 事件委托 click → 翻转对应 toggle → 按钮 on 类切换 →
  写 localStorage（try/catch）→ `klRender()`。绑定放 `klBindEvents` 或既有
  一次性绑定区（2770-2771 行 `window._klBound` 模式）。
- **klDrawMain lines 组装改造**（2384-2395 行）：
  - 删除 `marks.cost` 行（成本不再画线）；
  - levels 段包进 `if (KL.toggles.levels)`；
  - fib 段包进 `if (KL.toggles.fib)`；
  - stop_level + hhv22 两行包进 `if (KL.toggles.guard)`；
  - `lines.forEach` 拉伸 lo/hi 逻辑不变（lines 里只剩可见线，天然满足决定 5）。
- **klShowTip 加成本行**（2745-2754 行）：在「量/tick数」行后追加
  `(d.marks && d.marks.cost != null ? '成本 ' + fmt2(d.marks.cost) + '<br>' : '')`。

## 4. 边界

- localStorage 不可用（隐私模式）：try/catch 吞异常，功能退化为内存态开关，无报错。
- 无持仓成本标的（指数等 `marks.cost == null`）：klTip 不显示成本行。
- 周/月/分时：开关无效果（无线可画），按钮仍可点。
- 存储值损坏（旧格式/手改）：形状校验失败回退全关并覆写。

## 5. 测试

- 前端无测试基建（仓库惯例）：`node --check` 抽 script 块语法自检
  + 全量 pytest 回归（后端不动，预期 479 passed 不变）。
- 线上部署后人工浏览器验证清单：
  1. 打开任一持仓日K：默认三条标注组全关，图面只有蜡烛+均线+买卖点；
  2. 点亮「档位」→ ±X% 虚线出现且 y 轴收窄到蜡烛+档位范围；再点关闭恢复；
  3. 点亮「Fib」/「止损/22日高」同验证；
  4. 悬浮任一蜡烛：klTip 出现「成本 xxx」行（有持仓成本标的）；
  5. 刷新页面重开弹层：开关状态保持（localStorage）；
  6. 买卖点 ▲▼ 全程常驻。

## 6. 非目标

- MA50/MA200 均线开关（用户未要求，保持常显）。
- 逐条档位线粒度控制。
- 后端 /api/kline 任何改动。
