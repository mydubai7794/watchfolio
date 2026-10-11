# K线标注线开关 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** K线弹层主图右侧标注虚线（档位/Fib/止损/22日高）改为三组独立 toggle 选中才展示（localStorage 记忆、默认全关），成本线移出图面改在悬浮浮层 klTip 显示。

**Architecture:** 纯前端单文件改动（static/portfolio.html）：KL 状态对象加 `toggles` 三布尔组 + localStorage 读写（形状校验/隐私模式降级）；`klDrawMain` 的 lines 组装按组门控（y 轴 lo/hi 只由可见线拉伸）；`klShowTip` 追加成本行。开关切换走 `klRender()` 客户端重绘零请求。

**Tech Stack:** 原生 JS（单文件无构建）、canvas 2D、localStorage。后端 API 零改动。

**Spec:** docs/superpowers/specs/2026-10-11-kline-mark-toggles-design.md

---

### Task 1: 前端实现（HTML 按钮行 + toggles 状态 + klDrawMain 门控 + klTip 成本行）

**Files:**
- Modify: `static/portfolio.html`（四处编辑）

前端无测试基建（仓库惯例）：验证 = `node --check` 语法自检 + 全量 pytest 回归（后端不动应 479 passed）+ Task 2 线上浏览器验证。

- [ ] **Step 1: HTML——主图 canvas 上方插入开关按钮行**

先 Read `static/portfolio.html` 570-580 行确认锚点。在 `<canvas id="klMain"></canvas>`（约 571 行）**之前**插入：

```html
    <div class="kline-intabs kline-marks" id="klMarkToggles">
      <button data-mk="levels">档位</button>
      <button data-mk="fib">Fib</button>
      <button data-mk="guard">止损/22日高</button>
    </div>
```

（复用 `.kline-intabs` 既有样式：flex/按钮/`.on` 选中态/margin 均已定义于 294-305 行；`kline-marks` 类仅语义锚，CSS 不新增。）

- [ ] **Step 2: JS——KL 加 toggles 字段 + 读写/同步函数**

先 Read `static/portfolio.html` 2181-2186 行确认 KL 定义。改动两处：

2a. `KL` 对象字面量加一行（`_tradesSig` 之前）：

```js
  toggles: {levels: false, fib: false, guard: false},  // 标注线组开关：默认全关，localStorage 记忆
```

2b. 在 `KL` 定义结束（`};`）之后、`klLoad` 函数之前插入三个函数：

```js
/**
 * 标注线组开关加载：localStorage 持久化（key klMarkToggles，JSON 三布尔对象）。
 * 形状校验失败/隐私模式抛异常 → 保持默认全关，无报错。@author yys
 */
function klLoadToggles() {
  try {
    const v = JSON.parse(localStorage.getItem('klMarkToggles') || '');
    if (v && typeof v === 'object' && typeof v.levels === 'boolean' &&
        typeof v.fib === 'boolean' && typeof v.guard === 'boolean') {
      KL.toggles = {levels: v.levels, fib: v.fib, guard: v.guard};
    }
  } catch (e) { /* localStorage 不可用/值损坏：保持默认全关 */ }
  klSyncToggles();
}

/** 三个开关按钮选中态与 KL.toggles 同步。@author yys */
function klSyncToggles() {
  document.querySelectorAll('#klMarkToggles button').forEach(b =>
    b.classList.toggle('on', !!KL.toggles[b.dataset.mk]));
}

/** 开关状态写回 localStorage（隐私模式写失败静默降级为内存态）。@author yys */
function klSaveToggles() {
  try { localStorage.setItem('klMarkToggles', JSON.stringify(KL.toggles)); }
  catch (e) { /* 写失败忽略 */ }
}
```

- [ ] **Step 3: JS——事件绑定 + 初始化调用**

先 Read `static/portfolio.html` 2768-2772 行（「弹层初始化：事件只绑一次」注释区）。在 `if (!window._klBound) { klBindEvents(); window._klBound = true; }`（约 2771 行）**之后**插入：

```js
// 标注线组开关：点击切换 → 重绘（klRender 对空数据早退，弹层未开时安全）；状态写 localStorage
document.getElementById('klMarkToggles').addEventListener('click', e => {
  const b = e.target.closest('button');
  if (!b || !KL.toggles.hasOwnProperty(b.dataset.mk)) return;
  KL.toggles[b.dataset.mk] = !KL.toggles[b.dataset.mk];
  b.classList.toggle('on', KL.toggles[b.dataset.mk]);
  klSaveToggles();
  klRender();
});
klLoadToggles();
```

- [ ] **Step 4: JS——klDrawMain lines 组装按组门控 + 函数头注释更新**

先 Read `static/portfolio.html` 2371-2396 行。两处改动：

4a. 函数头 docstring（2372-2374 行）中「+ 成本/档位/止损/22日高标注虚线」改为「+ 档位/止损/22日高标注虚线（按组开关选中才画，KL.toggles；成本线已移 klTip 悬浮显示）」。

4b. lines 组装段（`const lines = [];` 至 fib forEach 结束）整段替换为：

```js
  // 标注线按组开关门控（默认全关，localStorage 记忆）；成本线不画图（klTip 悬浮显示）；
  // y 轴 lo/hi 由下方 lines.forEach 拉伸——lines 只含可见线，隐藏组不撑开图面
  const lines = [];
  if (KL.toggles.levels) (marks.levels || []).forEach(v => lines.push(
    [(v.pct > 0 ? '+' : '') + v.pct + '%', +v.price, v.pct > 0 ? DOWN_COLOR : TXT2]));
  if (KL.toggles.guard) {
    if (marks.stop_level != null) lines.push(['止损', +marks.stop_level, UP_COLOR]);
    if (marks.hhv22 != null) lines.push(['22日高', +marks.hhv22, '#2A6CF0']);
  }
  if (KL.toggles.fib) {
    const fib = ov && ov.fib;
    if (fib && fib.levels) {
      [['Fib38.2', fib.levels['382']], ['Fib50', fib.levels['500']],
       ['Fib61.8', fib.levels['618']]]
        .forEach(([nm, p]) => { if (p != null) lines.push([nm, +p, '#8E9ABF']); });
    }
  }
```

（其后 `lines.forEach(([, p]) => { lo = Math.min(lo, p); hi = Math.max(hi, p); });` 保持原样不动。）

- [ ] **Step 5: JS——klShowTip 加成本行**

先 Read `static/portfolio.html` 2745-2754 行。在「量/tick数」行（`+ '<br>'` 结尾）之后、「RSI」行之前插入一行：

```js
    (d.marks && d.marks.cost != null ? '成本 ' + fmt2(d.marks.cost) + '<br>' : '') +
```

（`d.marks` 非 day 周期为 undefined，`d.marks &&` 短路安全；cost 为 null 的标的不显示该行。）

- [ ] **Step 6: 语法自检 + 后端回归**

Run: `node --check <(sed -n '/<script>/,/<\/script>/p' static/portfolio.html | sed '1d;$d') 2>/dev/null || echo "node 不可用，改用目测"`
Expected: 无输出（语法通过）

Run: `python3 -m pytest tests/test_closed_positions.py tests/test_alert_api.py -q`
Expected: 46 passed（前端改动不影响后端）

- [ ] **Step 7: 提交**

```bash
git add static/portfolio.html
git commit -m "feat: K线标注线三组开关（档位/Fib/止损22日高，默认全关localStorage记忆，y轴随可见线收窄），成本线移klTip悬浮显示"
```

---

### Task 2: 部署 + 线上验证

**Files:**
- 无代码改动（rsync 部署 + 验证）

- [ ] **Step 1: secrets 门禁 + 部署**

Run: `python3 scripts/check_secrets.py`
Expected: 检出 0 处

部署命令（真实 IP 见项目根 `AGENTS.local.md`，rsync 模板同 AGENTS.md 部署节）：

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  --exclude='qdii.env' --exclude='*.local.md' --exclude='.mcp.json' \
  --exclude='static/download' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"
```

- [ ] **Step 2: 服务冒烟**

1. `ssh root@<server-ip> "systemctl is-active qdii"` → active
2. `ssh root@<server-ip> "journalctl -u qdii --since '2 min ago' --no-pager | tail -20"` → 无 Traceback
3. 服务器本机（127.0.0.1:8888）带 session cookie（sessions 表最新有效 token）curl：`/api/kline/QQQ?tf=day` 响应正常（marks/overlays 仍在 payload，前端门控不删后端数据）；首页 200

- [ ] **Step 3: 输出人工浏览器验证清单（留给用户）**

1. 打开任一持仓日K：默认三组开关全关，主图只有蜡烛+均线+买卖点，右侧无标注虚线
2. 点亮「档位」→ ±X% 虚线出现、y 轴收窄；再点关闭恢复干净
3. 点亮「Fib」「止损/22日高」同验证
4. 悬浮蜡烛：klTip 出现「成本 xxx」行（有持仓成本标的；指数/无成本标的不显示）
5. 刷新页面重开弹层：开关状态保持（localStorage）
6. 买卖点 ▲▼ 常驻不受开关影响

---

## 自审记录（writing-plans Self-Review）

1. **Spec 覆盖**：§2.1 三组开关→Step 1/4；§2.2 localStorage→Step 2/3；§2.3 成本 klTip→Step 5；§2.4 买卖点常驻→klDrawMain 标记分支不触碰（Step 4 只改 lines 段）；§2.5 y 轴可见线→Step 4b 注释+原 forEach 不动；§2.6 零请求→Step 3 klRender；§2.7 按钮样式→Step 1 复用；§2.8 周月分时无副作用→klRender/klDrawMain null 安全既有；§2.9 MA/后端不动→无对应步骤（约束达成）。§4 边界：形状校验→Step 2b try/catch+三布尔检查；隐私模式→catch 吞。§5 测试→Step 6 + Task 2。无缺口。
2. **占位符扫描**：全部步骤含完整代码/命令/期望输出，无 TBD/TODO。
3. **类型一致性**：`KL.toggles` 字段名（levels/fib/guard）在 Step 2a 定义、2b 读取、3 翻转、4b 门控一致；`data-mk` 属性值与字段名一致；`klLoadToggles/klSyncToggles/klSaveToggles` 命名三处一致。
