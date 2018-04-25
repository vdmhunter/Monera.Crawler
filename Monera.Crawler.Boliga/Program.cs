using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using HtmlAgilityPack;
using ShellProgressBar;

namespace Monera.Crawler.Boliga
{
    class Program
    {
        private static string _startUrl;
        private static string _siteUrl;
        private static Guid _searchGuid;
        private static List<int> _pageNumbers;
        private static Dictionary<string, BoligaProperty> _properties;
        private static int _totalPropertiesCount;
        private static int _counter;
        private static ProgressBar _pbar;
        private static readonly BoligaDBEntities _db = new BoligaDBEntities();

        private static int GetTotalPropertiesCount(HtmlDocument doc)
        {
            int result = 0;
            CultureInfo danishCulture = new CultureInfo("da-DK");

            HtmlNode hn = doc.DocumentNode.SelectSingleNode("//table[@class='searchresultpaging'][1]/tr/td[2]/label[1]/p");
            string totalPropertiesCountStr =
                    hn?.InnerHtml
                        .Split(new string[] { " af ialt " }, StringSplitOptions.None)[1]
                        .Split(' ')[0]
                        .Trim();

            result = totalPropertiesCountStr != null
                ? int.Parse(totalPropertiesCountStr, NumberStyles.Number, danishCulture)
                : 0;

            return result;
        }

        private static List<int> GetNewSearchPageNumbers(HtmlDocument doc)
        {
            List<int> result = new List<int>();

            HtmlNodeCollection hnc = doc.DocumentNode.SelectNodes("//table[@class='searchresultpaging'][1]/tr/td[2]/a");
            HtmlNode hn = doc.DocumentNode.SelectSingleNode("//table[@class='searchresultpaging'][1]/tr/td[2]/a/p/b");
            int currentPageNumber = Convert.ToInt32(hn.InnerText);
            bool flag = false;
            foreach (int pageNumber in hnc.Select(n => Convert.ToInt32(n.InnerText)))
            {
                if (pageNumber == currentPageNumber)
                    flag = true;
                if (pageNumber != currentPageNumber && !_pageNumbers.Contains(pageNumber) && flag)
                {
                    lock (_pageNumbers)
                    {
                        _pageNumbers.Add(pageNumber);
                        result.Add(pageNumber);
                    }
                }
            }

            return result;
        }

        private static List<string> GetProrertyUrls(HtmlDocument doc)
        {
            List<string> result = new List<string>();

            HtmlNodeCollection hnc = doc.DocumentNode.SelectNodes("//table[@id='searchtable']/tr[@class='pRow ' or @class='pRow even']");
            if (hnc != null)
            {
                foreach (HtmlNode row in hnc)
                {
                    HtmlNode atag = row.SelectSingleNode("td/a[1]");
                    result.Add(atag.Attributes["href"].Value);
                }
            }

            hnc = doc.DocumentNode.SelectNodes("//table[@id='searchtable']/tr[@class='pRow enhanced']");
            if (hnc != null)
            {
                foreach (HtmlNode row in hnc)
                {
                    HtmlNode atag = row.SelectSingleNode("td[1]/table[@class='searchResultTable']/tr/td[@class='value']/div[@class='title']/a");
                    result.Add(atag.Attributes["href"].Value);
                }
            }

            return result;
        }

        private static void GetPropertyData(string url)
        {
            BoligaProperty result = new BoligaProperty
            {
                SogeresultaterGuid = _searchGuid
                ,Link = $"http://{_siteUrl}{url}"
            };
            CultureInfo danishCulture = new CultureInfo("da-DK");

            using (var client = new HttpClient())
            {
                var uri = new Uri($"http://{_siteUrl}/{url}");

                var response = client.GetAsync(uri).Result;
                var responseString = response.Content.ReadAsStringAsync().Result;

                HtmlDocument htmlDoc = new HtmlDocument { OptionFixNestedTags = true };
                htmlDoc.LoadHtml(responseString);

                HtmlNode titelNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='main-content paddingT']/h2[1]");
                string titelStr =
                    titelNode?.InnerText
                        .Trim();
                string titel = titelNode != null ? WebUtility.HtmlDecode(titelStr) : null;

                HtmlNode postnrNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='main-content paddingT']/h3[1]");
                string postnrStr = null;
                if (postnrNode?.InnerText.Split(' ')[0].Length > 1)
                {
                    postnrStr = postnrNode?.InnerText
                        .Split(' ')[0]
                        .Trim();
                }
                string postnr = postnrNode != null ? WebUtility.HtmlDecode(postnrStr) : null;

                HtmlNode postnrTitelNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='main-content paddingT']/h3[1]");
                string postnrTitelStr = null;
                if (postnrTitelNode?.InnerText.Split(' ')[1].Length > 1)
                {
                    postnrTitelStr = postnrTitelNode?.InnerText
                        .Split(' ')[1]
                        .Trim();
                }
                string postnrTitel = postnrNode != null ? WebUtility.HtmlDecode(postnrTitelStr) : null;

                HtmlNode kontantprisNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr[1]/td[1]/strong");
                string kontantprisSrt = null;
                if (kontantprisNode?.InnerText.Split(' ').Length > 1)
                {
                    kontantprisSrt = kontantprisNode?.InnerText.Split(' ')[0];
                }
                decimal? kontantpris = kontantprisSrt != null ? decimal.Parse(kontantprisSrt, danishCulture) : (decimal?)null;

                HtmlNode ejerudgiftNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//*[text()[contains(.,'Ejerudgift')]]");
                string ejerudgiftStr = null;
                if (ejerudgiftNode?.InnerHtml.Split(new string[] {"<br>\r\n"}, StringSplitOptions.None).Length > 1
                    && ejerudgiftNode?.InnerHtml
                    .Split(new string[] { "<br>\r\n" }, StringSplitOptions.None)[1]
                    .Split(new string[] { "<small>" }, StringSplitOptions.None).Length > 1)
                {
                    ejerudgiftStr =
                    ejerudgiftNode?.InnerHtml
                        .Split(new string[] { "<br>\r\n" }, StringSplitOptions.None)[1]
                        .Split(new string[] { "<small>" }, StringSplitOptions.None)[0]
                        .Trim();
                }
                decimal? ejerudgift = ejerudgiftStr != null ? decimal.Parse(ejerudgiftStr, danishCulture) : (decimal?) null;

                HtmlNode kvmprisNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Kvmpris']/following-sibling::td");
                string kvmprisStr = null;
                if (kvmprisNode?.InnerText.Split(new string[] { "<small>" }, StringSplitOptions.None).Length > 1
                    && kvmprisNode?.InnerText.Split(new string[] { "<small>" }, StringSplitOptions.None)[0]
                    .Split(new string[] { "kr./m&sup2;" }, StringSplitOptions.None).Length > 1)
                {
                    kvmprisStr = kvmprisNode?.InnerText
                    .Split(new string[] { "<small>" }, StringSplitOptions.None)[0]
                    .Split(new string[] { "kr./m&sup2;" }, StringSplitOptions.None)[0]
                    .Trim();
                }
                decimal? kvmpris = kvmprisStr != null ? decimal.Parse(kvmprisStr, danishCulture) : (decimal?) null;

                HtmlNode typeNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Type']/following-sibling::td");
                string type = typeNode != null ? WebUtility.HtmlDecode(typeNode.InnerText) : null;

                HtmlNode boligNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Bolig']/following-sibling::td");
                string boligStr = boligNode?.InnerText
                    .Split(new string[] { "m&sup2;" }, StringSplitOptions.None)[0]
                    .Trim();
                int? bolig = boligStr != null ? int.Parse(boligStr, NumberStyles.Number, danishCulture) : (int?)null;

                HtmlNode grundNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Grund']/following-sibling::td");
                string grundStr = grundNode?.InnerText
                    .Split(new string[] { "m&sup2;" }, StringSplitOptions.None)[0]
                    .Trim();
                int? grund = grundStr != null ? int.Parse(grundStr, NumberStyles.Number, danishCulture) : (int?)null;

                HtmlNode vaerelserNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Værelser']/following-sibling::td");
                string vaerelserStr = vaerelserNode?.InnerText
                    .Trim();
                int? vaerelser = vaerelserStr != null ? int.Parse(vaerelserStr, NumberStyles.Number, danishCulture) : (int?)null;

                HtmlNode etageNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Etage']/following-sibling::td");
                string etage = etageNode != null ? WebUtility.HtmlDecode(etageNode.InnerText.Trim()) : null;

                HtmlNode byggearNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Byggeår']/following-sibling::td");
                string byggearStr = byggearNode?.InnerText
                    .Trim();
                int? byggear = byggearStr != null ? int.Parse(byggearStr, NumberStyles.Number, danishCulture) : (int?)null;

                HtmlNode oprettetNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Oprettet']/following-sibling::td");
                string oprettetStr = oprettetNode?.InnerText
                    .Trim();
                DateTime? oprettet = oprettetStr != null ? DateTime.ParseExact(oprettetStr, "dd-MM-yyyy", CultureInfo.InvariantCulture) : (DateTime?)null;

                HtmlNode liggetidNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//table[@class='table estate-table']/tr/td[.='Liggetid']/following-sibling::td");
                string liggetidStr = liggetidNode?.InnerText
                    .Split(' ')[0];
                int? liggetid = liggetidStr != null ? int.Parse(liggetidStr, NumberStyles.Number, danishCulture) : (int?)null;

                HtmlNode brokerLinkNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//a[@class='but brokerLink']");
                string brokerLink = brokerLinkNode?.Attributes["href"].Value
                    .Trim();

                HtmlNode butikTitelNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='estate-50 margin-top margin-right agent-box fill']/div[@class='wrapper']/strong");
                string butikTitel = butikTitelNode != null ? WebUtility.HtmlDecode(butikTitelNode.InnerText.Trim()) : null;

                HtmlNode butikAdresseNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='estate-50 margin-top margin-right agent-box fill']/div[@class='wrapper']");
                string butikAdresseStr = null;
                if (butikAdresseNode?.InnerHtml.Split(new string[] {"</strong><br>"}, StringSplitOptions.None).Length > 1
                    && butikAdresseNode?.InnerHtml.Split(new string[] { "<br>" }, StringSplitOptions.None).Length > 1)
                {
                    butikAdresseStr = butikAdresseNode?.InnerHtml
                        .Split(new string[] { "</strong><br>" }, StringSplitOptions.None)[1]
                        .Split(new string[] { "<br>" }, StringSplitOptions.None)[0]
                        .Trim();
                }
                string butikAdresse = butikAdresseStr != null ? WebUtility.HtmlDecode(butikAdresseStr.Trim()) : null;

                HtmlNode butikPostnrNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='estate-50 margin-top margin-right agent-box fill']/div[@class='wrapper']");
                string butikPostnrStr = null;
                if (butikPostnrNode?.InnerHtml.Split(new string[] { "</strong><br>" }, StringSplitOptions.None).Length > 1
                    && butikPostnrNode?.InnerHtml.Split(new string[] { "</strong><br>" }, StringSplitOptions.None)[1]
                    .Split(new string[] { "<br>" }, StringSplitOptions.None).Length > 1)
                {
                    butikPostnrStr = butikPostnrNode?.InnerHtml
                    .Replace("\r\n", "")
                    .Trim()
                    .Split(new string[] { "</strong><br>" }, StringSplitOptions.None)[1]
                    .Split(new string[] { "<br>" }, StringSplitOptions.None)[1]
                    .Trim()
                    .Split(' ')[0]
                    .Trim();
                }
                string butikPostnr = butikPostnrStr;

                HtmlNode butikPostnrTitelNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='estate-50 margin-top margin-right agent-box fill']/div[@class='wrapper']");
                string butikPostnrTitelStr = null;
                if (butikPostnrTitelNode?.InnerHtml.Replace("\r\n", "").Trim().Split(new string[] { "</strong><br>" }, StringSplitOptions.None).Length > 1
                    && butikPostnrTitelNode?.InnerHtml.Replace("\r\n", "").Trim().Split(new string[] { "</strong><br>" }, StringSplitOptions.None)[1]
                    .Split(new string[] { "<br>" }, StringSplitOptions.None).Length > 1
                    && butikPostnrTitelNode?.InnerHtml.Replace("\r\n", "").Trim().Split(new string[] { "</strong><br>" }, StringSplitOptions.None)[1]
                    .Split(new string[] { "<br>" }, StringSplitOptions.None)[1].Split(' ')[1].Length > 1)
                {
                    butikPostnrTitelStr = butikPostnrTitelNode?.InnerHtml
                    .Replace("\r\n", "")
                    .Trim()
                    .Split(new string[] { "</strong><br>" }, StringSplitOptions.None)[1]
                    .Split(new string[] { "<br>" }, StringSplitOptions.None)[1]
                    .Trim()
                    .Split(' ')[1]
                    .Trim();
                }
                string butikPostnrTitel = butikPostnrTitelStr;

                HtmlNode prisforskelProcentdelNode =
                    htmlDoc.DocumentNode.SelectSingleNode(
                        "//div[@class='estate-50 margin-top gauge-box fill']/div[@class='wrapper']/strong");
                int? prisforskelProcentdel = null;
                if (prisforskelProcentdelNode?.InnerText.Split(new string[] { "% " }, StringSplitOptions.None).Length > 1
                    && prisforskelProcentdelNode?.InnerText.Split(new string[] { "% " }, StringSplitOptions.None).Length > 1)
                {
                    string prisforskelProcentdelNumber = prisforskelProcentdelNode.InnerText
                    .Split(new string[] { "% " }, StringSplitOptions.None)[0];
                    string prisforskelProcentdelPlusMinus = prisforskelProcentdelNode.InnerText
                        .Split(new string[] { "% " }, StringSplitOptions.None)[1];
                    prisforskelProcentdelNumber = prisforskelProcentdelPlusMinus == "lavere" ? $"-{prisforskelProcentdelNumber}" : prisforskelProcentdelNumber;
                    prisforskelProcentdel = prisforskelProcentdelNumber != null ? int.Parse(prisforskelProcentdelNumber, NumberStyles.Number, danishCulture) : (int?)null;
                }

                HtmlNode kvmprisBoligenNode =
                    htmlDoc.DocumentNode.SelectSingleNode("//table[@class='table table-compare']/tr/td[contains(.,'Kvmpris boligen')]/following-sibling::td/strong");
                string kvmprisBoligenStr = kvmprisBoligenNode?.InnerText;
                decimal? kvmprisBoligen = kvmprisBoligenStr != null ? decimal.Parse(kvmprisBoligenStr, danishCulture) : (decimal?)null;

                HtmlNode kvmprisOmradetNode =
                    htmlDoc.DocumentNode.SelectSingleNode("//table[@class='table table-compare']/tr/td[contains(.,'Kvmpris området')]/following-sibling::td/strong");
                string kvmprisOmradetNodeStr = kvmprisOmradetNode?.InnerText;
                decimal? kvmprisOmradet = kvmprisOmradetNodeStr != null ? decimal.Parse(kvmprisOmradetNodeStr, danishCulture) : (decimal?)null;

                result.Titel = titel;
                result.Postnr = postnr;
                result.PostnrTitel = postnrTitel;
                result.Kontantpris = kontantpris;
                result.Ejerudgift = ejerudgift;
                result.Kvmpris = kvmpris;
                result.Type = type;
                result.Bolig = bolig;
                result.Grund = grund;
                result.Vaerelser = vaerelser;
                result.Etage = etage;
                result.Byggear = byggear;
                result.Oprettet = oprettet;
                result.Liggetid = liggetid;
                result.BrokerLink = brokerLink;
                result.ButikTitel = butikTitel;
                result.ButikAdresse = butikAdresse;
                result.ButikPostnr = butikPostnr;
                result.ButikPostnrTitel = butikPostnrTitel;
                result.PrisforskelProcentdel = prisforskelProcentdel;
                result.KvmprisBoligen = kvmprisBoligen;
                result.KvmprisOmradet = kvmprisOmradet;
            }

            lock ( _properties)
            {
                _properties.Add(url, result);
            }
        }

        private static async Task<bool> GetSearchPage(int pageNumber)
        {
            using (var client = new HttpClient())
            {
                var uri = new Uri($"{_startUrl}?page={pageNumber}");

                var response = await client.GetAsync(uri);
                var responseString = await response.Content.ReadAsStringAsync();

                HtmlDocument htmlDoc = new HtmlDocument {OptionFixNestedTags = true};
                htmlDoc.LoadHtml(responseString);

                if (pageNumber == 1)
                {
                    _totalPropertiesCount = GetTotalPropertiesCount(htmlDoc);
                    _pbar = new ProgressBar(_totalPropertiesCount, "Starting...", ConsoleColor.Cyan, '\u2593');
                }

                List<int> newPageNumbers = GetNewSearchPageNumbers(htmlDoc);
                if (newPageNumbers == null)
                    return true;
                else
                {
                    Parallel.ForEach(GetProrertyUrls(htmlDoc), GetPropertyData);
                    lock (_properties)
                    {
                        foreach (var p in _properties)
                            _db.BoligaProperty.Add(p.Value);
                        _db.SaveChanges();
                        _counter += _properties.Count;
                        for (int i = 1; i < _properties.Count; i++)
                        {
                            _pbar.Tick("Properties processed " + _counter);
                        }
                        _properties.Clear();
                    }

                    foreach (int pn in newPageNumbers)
                    {
                        await GetSearchPage(pn);
                    }
                }
            }

            return true;
        }

        static void Main(string[] args)
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Green;
                DateTime startDateTime = DateTime.Now;
                Console.WriteLine($"Boliga crawler started at - {startDateTime}");
                Console.WriteLine();

                _startUrl = ConfigurationManager.AppSettings["startUrl"];
                _siteUrl = new Uri(_startUrl).Host;
                _searchGuid = new Guid(_startUrl.Split('/').Last());
                _pageNumbers = new List<int>();
                _properties = new Dictionary<string, BoligaProperty>();
                _counter = 0;
                _totalPropertiesCount = 0;

                Task.Run(async () =>
                {
                    await GetSearchPage(1);
                }).Wait();

                lock (_properties)
                {
                    foreach (var p in _properties)
                        _db.BoligaProperty.Add(p.Value);
                    _properties.Clear();
                }

                _db.SaveChanges();

                _pbar.Dispose();
                Console.Clear();
                Console.WriteLine($"Boliga crawler started at - {startDateTime}");
                Console.WriteLine($"Boliga crawler stopped at - {DateTime.Now}");
                Console.WriteLine("Press any key...");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                _pbar?.Dispose();
                Console.SetCursorPosition(0, 6);
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                Console.WriteLine("Press any key...");
                Console.ReadLine();
            }
        }
    }
}
