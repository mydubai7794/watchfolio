using System;
using System.Drawing;
using System.Net.Http;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace StockWatcher
{
    public partial class StockPreviewControl : UserControl
    {
        private Timer timer = null;
        private static int currentIndex = -1;
        private static int maxIndex = -1;
        private int errorCount = 0;
        public StockPreviewControl(CSDeskBand.CSDeskBandWin w)
        {
            CheckForIllegalCrossThreadCalls = false;

            InitializeComponent();

            this.ContextMenu = new ContextMenu(new MenuItem[]
            {
//                new MenuItem("联系作者", new EventHandler((s, e) =>
//                {
//                    Util.Info($@"

// System.Windows.Forms.Application.ExecutablePath:{System.Windows.Forms.Application.ExecutablePath}
//System.Windows.Forms.Application.StartupPath:{System.Windows.Forms.Application.StartupPath}
//System.AppDomain.CurrentDomain.BaseDirectory:{System.AppDomain.CurrentDomain.BaseDirectory}");
//                })),
                new MenuItem("设置", new EventHandler((s, e) =>
                {
                    new frmSetting(this).ShowDialog();
                })),
            });
            ResetTimer();

            w.ShowDW(true);
        }

        public void ResetTimer()
        {
            if (timer != null)
            {
                timer.Stop();
                timer.Tick -= Timer_Tick;
                timer.Dispose();
                currentIndex = maxIndex = -1;
                timer = null;
            }
            timer = new Timer()
            {
                Interval = StockConfig.RefreshInterval,
                Enabled = true
            };
            timer.Tick += Timer_Tick;
            timer.Start();
        }

        private void Timer_Tick(object sender, EventArgs e)
        {
            LoadSetting();
            if (currentIndex == -1)
            {
                UpdateStatus($"暂无监视对象");
            }
            else
            {
                Task.Run(async () =>
                {
                    var currentCode = StockConfig.StockList[currentIndex];
                    var quoteModel = await GetQuoteInfo(currentCode);
                    if (quoteModel == null)
                    {
                        UpdateStatus($"查询{currentCode}失败");
                    }
                    else
                    {
                        labelForStatus.Tag = quoteModel;
                        UpdateStatus(quoteModel.DisplayText
                            , quoteModel.IsUp ? StockColor.Red : StockColor.Green);
                    }
                });
            }
        }

        private void UpdateStatus(string text, StockColor color = StockColor.Disabled)
        {
            switch (color)
            {
                case StockColor.Green:
                    labelForStatus.ForeColor = Color.FromArgb(0, 120, 0);
                    break;
                case StockColor.Red:
                    labelForStatus.ForeColor = Color.FromArgb(120, 0, 0);
                    break;
                case StockColor.Disabled:
                    labelForStatus.ForeColor = Color.White;
                    labelForStatus.Tag = null;
                    break;
            }
            labelForStatus.Text = text;
        }

        private void LoadSetting()
        {
            StockConfig.LoadSetting();
            maxIndex = StockConfig.StockList.Count-1;
            if (maxIndex == 0)
            {
                currentIndex = 0;
            }else if (maxIndex < 0)
            {
                currentIndex = -1;
            }
            else if (currentIndex > maxIndex)
            {
                currentIndex = 0;
            }
            else
            {
                ++currentIndex;
            }
        }

        private async Task<QuoteModel> GetQuoteInfo(string code)
        {
            try
            {
                var res = await InvokeQdiiApi(code);
                if (string.IsNullOrEmpty(res))
                {
                    throw new Exception("qdii API 返回空响应");
                }
                var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
                var json = serializer.Deserialize<Dictionary<string, object>>(res);
                var items = json["items"] as System.Collections.ArrayList;
                if (items == null || items.Count == 0)
                {
                    throw new Exception($"代码 {code} 未在候选池中找到");
                }
                var item = (Dictionary<string, object>)items[0];
                return new QuoteModel()
                {
                    Code = item.ContainsKey("code") ? (item["code"]?.ToString() ?? code) : code,
                    Name = item.ContainsKey("name") ? (item["name"]?.ToString() ?? "") : "",
                    Type = item.ContainsKey("type") ? (item["type"]?.ToString() ?? "stock") : "stock",
                    CurrentPrice = item.ContainsKey("price") ? float.TryParse(item["price"]?.ToString(), out var p) ? p : 0 : 0,
                    LastClose = item.ContainsKey("pre_close") ? float.TryParse(item["pre_close"]?.ToString(), out var pc) ? pc : 0 : 0,
                    ChangePct = item.ContainsKey("change_pct") ? float.TryParse(item["change_pct"]?.ToString(), out var cp) ? cp : 0 : 0,
                    Iopv = item.ContainsKey("iopv") ? float.TryParse(item["iopv"]?.ToString(), out var iv) ? iv : 0 : 0,
                    PremiumPct = item.ContainsKey("premium_pct") ? float.TryParse(item["premium_pct"]?.ToString(), out var pp) ? pp : 0 : 0,
                    BuyPrice1 = item.ContainsKey("buy1_price") ? float.TryParse(item["buy1_price"]?.ToString(), out var bp) ? bp : 0 : 0,
                    BuyVolume1 = item.ContainsKey("buy1_vol") ? float.TryParse(item["buy1_vol"]?.ToString(), out var bv) ? bv : 0 : 0,
                    SellPrice1 = item.ContainsKey("sell1_price") ? float.TryParse(item["sell1_price"]?.ToString(), out var sp) ? sp : 0 : 0,
                    SellVolume1 = item.ContainsKey("sell1_vol") ? float.TryParse(item["sell1_vol"]?.ToString(), out var sv) ? sv : 0 : 0,
                };
            }
            catch (Exception ex)
            {
                errorCount++;
                if (errorCount < 10)
                {
                    Util.Log($"查询行情【{code}】出错！", ex);
                }
                return null;
            }
        }

        private async Task<string> InvokeQdiiApi(string code)
        {
            var api = $"{StockConfig.QdiiApiUrl}?codes={code}";
            try
            {
                using (var client = new HttpClient()
                {
                    Timeout = TimeSpan.FromSeconds(5),
                })
                {
                    return await client.GetStringAsync(api);
                }
            }
            catch (Exception ex)
            {
                Util.Log($"请求qdii API【{api}】出错！", ex);
                return "";
            }
        }

        private void labelForStatus_DoubleClick(object sender, EventArgs e)
        {
            if (labelForStatus.Tag != null && labelForStatus.Tag is QuoteModel model)
            {
                Util.Info(model.DetailText, "查看详情");
            }
        }
    }

    class QuoteModel
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public float CurrentPrice { get; set; }
        public float LastClose { get; set; }
        public float ChangePct { get; set; }
        public float Iopv { get; set; }
        public float PremiumPct { get; set; }
        public float BuyPrice1 { get; set; }
        public float BuyVolume1 { get; set; }
        public float SellPrice1 { get; set; }
        public float SellVolume1 { get; set; }

        public bool IsUp
        {
            get { return LastClose > 0 && CurrentPrice >= LastClose; }
        }

        public string DisplayText
        {
            get
            {
                if (Type == "futures")
                {
                    return $"{Name} {(ChangePct >= 0 ? "+" : "")}{(ChangePct).ToString("F2")}%";
                }
                else if (Type == "etf" && Iopv > 0)
                {
                    return $"{Name} {CurrentPrice.ToString("F3")}\r\n{(PremiumPct >= 0 ? "+" : "")}{(PremiumPct).ToString("F2")}%";
                }
                else
                {
                    return $"{Name} {CurrentPrice.ToString("F2")}\r\n{(ChangePct >= 0 ? "+" : "")}{(ChangePct).ToString("F2")}%";
                }
            }
        }

        public string DetailText
        {
            get
            {
                var sb = new System.Text.StringBuilder();
                sb.AppendLine($"代码：{Code}");
                sb.AppendLine($"名称：{Name}");
                sb.AppendLine($"类型：{Type}");
                sb.AppendLine($"现价：{CurrentPrice}");
                if (Type == "futures")
                {
                    sb.AppendLine($"涨跌幅：{(ChangePct >= 0 ? "+" : "")}{ChangePct.ToString("F2")}%");
                }
                else
                {
                    sb.AppendLine($"昨收：{LastClose}");
                    sb.AppendLine($"涨跌幅：{(ChangePct >= 0 ? "+" : "")}{ChangePct.ToString("F2")}%");
                    if (Iopv > 0)
                    {
                        sb.AppendLine($"IOPV：{Iopv.ToString("F3")}");
                        sb.AppendLine($"溢价率：{(PremiumPct >= 0 ? "+" : "")}{PremiumPct.ToString("F2")}%");
                    }
                }
                return sb.ToString();
            }
        }
    }

    enum StockColor
    {
        Green,
        Red,
        Disabled
    }
}
