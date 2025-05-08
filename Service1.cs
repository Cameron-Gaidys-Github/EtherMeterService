using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using log4net;
using System.Data.SqlClient;
using System.Data.Common;
using System.Timers;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web;
using HtmlAgilityPack;
using System.Net.Mail;
using System.Net.Mime;

namespace EtherMeterService
{
    public partial class Service1 : ServiceBase
    {
        private static readonly ILog Log =
            LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        Timer ActionTimer = new Timer();

        public Service1()
        {
            InitializeComponent();
            ActionTimer.Interval = 60000;
            ActionTimer.Enabled = false;
            ActionTimer.Elapsed += ActionTimer_Tick;
        }

        void ActionTimer_Tick(object sender, EventArgs e)
        {
            ActionTimer.Stop();
            if (CheckTime() == false)
            {
                // Getting readings failed. Send an error message.
            }
            ActionTimer.Start();
        }

        bool CheckTime()
        {
            if (Globals.dtLastReadingTime == DateTime.MinValue)
            {
                Log.Info("The service has just started. Grab a set of readings from the meters.");
                if (GetReadingsFromMeters() == false)
                {
                    // Getting readings failed.
                }
                Globals.dtLastReadingTime = DateTime.Now;
            }
            else
            {
                if (DateTime.Now.Hour > Globals.dtLastReadingTime.Hour || Globals.dtLastReadingTime.Date < DateTime.Now.Date)
                {
                    Log.Info("It's time to get readings from the meters.");
                    if (GetReadingsFromMeters() == false)
                    {
                        // Getting readings failed.
                    }
                    Globals.dtLastReadingTime = DateTime.Now;
                }
            }
            return true;
        }

        string GetCurrentAdjustmentKey()
        {
            int currentWeek = GetCurrentWeekOfYear();
            string currentMonth = DateTime.Now.ToString("MMMM"); // e.g., "January"

            // Check for specific weeks
            if (currentWeek == 3) return "MLK Week";
            if (currentWeek == 51) return "Christmas Week";
            if (currentWeek == 7) return "Presidents Week";

            // Default to the current month
            return currentMonth;
        }

        Dictionary<string, float> GetThresholdAdjustments(int meterID)
        {
            Dictionary<string, float> adjustments = new Dictionary<string, float>();
            string connectionString = Globals.sDBConnectString;
            string strSQL = $"SELECT * FROM EthermeterThresholdOffset WHERE MeterID = {meterID}";

            Log.Info($"Querying EthermeterThresholdOffset database for MeterID: {meterID}");

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(strSQL, connection);
                    SqlDataReader rdr = command.ExecuteReader();

                    if (rdr.HasRows)
                    {
                        rdr.Read();
                        Log.Info($"Retrieved adjustment values for MeterID {meterID}:");
                        adjustments["Christmas Week"] = rdr["Christmas Week"] != DBNull.Value ? Convert.ToSingle(rdr["Christmas Week"]) : 0f;
                        adjustments["MLK Week"] = rdr["MLK Week"] != DBNull.Value ? Convert.ToSingle(rdr["MLK Week"]) : 0f;
                        adjustments["Presidents Week"] = rdr["Presidents Week"] != DBNull.Value ? Convert.ToSingle(rdr["Presidents Week"]) : 0f;
                        adjustments["January"] = rdr["January"] != DBNull.Value ? Convert.ToSingle(rdr["January"]) : 0f;
                        adjustments["February"] = rdr["February"] != DBNull.Value ? Convert.ToSingle(rdr["February"]) : 0f;
                        adjustments["March"] = rdr["March"] != DBNull.Value ? Convert.ToSingle(rdr["March"]) : 0f;
                        adjustments["April"] = rdr["April"] != DBNull.Value ? Convert.ToSingle(rdr["April"]) : 0f;
                        adjustments["May"] = rdr["May"] != DBNull.Value ? Convert.ToSingle(rdr["May"]) : 0f;
                        adjustments["June"] = rdr["June"] != DBNull.Value ? Convert.ToSingle(rdr["June"]) : 0f;
                        adjustments["July"] = rdr["July"] != DBNull.Value ? Convert.ToSingle(rdr["July"]) : 0f;
                        adjustments["August"] = rdr["August"] != DBNull.Value ? Convert.ToSingle(rdr["August"]) : 0f;
                        adjustments["September"] = rdr["September"] != DBNull.Value ? Convert.ToSingle(rdr["September"]) : 0f;
                        adjustments["October"] = rdr["October"] != DBNull.Value ? Convert.ToSingle(rdr["October"]) : 0f;
                        adjustments["November"] = rdr["November"] != DBNull.Value ? Convert.ToSingle(rdr["November"]) : 0f;
                        adjustments["December"] = rdr["December"] != DBNull.Value ? Convert.ToSingle(rdr["December"]) : 0f;

                        foreach (var adjustment in adjustments)
                        {
                            Log.Info($"  {adjustment.Key}: {adjustment.Value}");
                        }
                    }
                    else
                    {
                        Log.Info($"No adjustment values found for MeterID {meterID}.");
                    }
                    rdr.Close();
                }
                catch (Exception ex)
                {
                    Log.Error($"Error retrieving threshold adjustments for MeterID {meterID}. Exception: {ex}");
                }
            }

            return adjustments;
        }

        bool SendAlertEmail(int meterID, string emailMessage)
        {
            try
            {
                // Get the meter name from the dictionary
                string meterName = Globals.MeterNames.ContainsKey(meterID) ? Globals.MeterNames[meterID] : "Unknown Meter";

                // Include the meter name in the email body
                emailMessage = $"Alert for {meterName} (MeterID: {meterID}):\n" + emailMessage;

                // Configure the SMTP client with the provided settings
                SmtpClient mySmtpClient = new SmtpClient("smtp-relay.idirectory.itw")
                {
                    Port = 25, // Use port 25
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = true // Enable anonymous authentication
                };

                // Create the email message
                MailMessage mail = new MailMessage
                {
                    From = new MailAddress("EthermeterAlerts@sugarbush.com"),
                    Subject = "Alert Notification",
                    Body = emailMessage,
                    IsBodyHtml = false
                };

                // Add recipient(s)
                mail.To.Add("Waterusagealerts@sugarbush.com");

                // Send the email
                mySmtpClient.Send(mail);

                Log.Info("Alert email sent successfully.");
            }
            catch (Exception ex)
            {
                Log.Error("Error sending alert email. " + ex.ToString());
            }

            return true;
        }


        int GetCurrentWeekOfYear()
        {
            var culture = System.Globalization.CultureInfo.CurrentCulture;
            var calendar = culture.Calendar;
            var dateTimeFormat = culture.DateTimeFormat;
            return calendar.GetWeekOfYear(DateTime.Now, dateTimeFormat.CalendarWeekRule, dateTimeFormat.FirstDayOfWeek);
        }

        private bool RecordNewReadings()
        {
            Log.Info("=== Starting Reading Insert and Threshold Analysis ===");

            try
            {
                using (SqlConnection connection = new SqlConnection(Globals.sDBConnectString))
                {
                    connection.Open();

                    foreach (var kvp in Globals.dicMeterNetFaults)
                    {
                        if (!Globals.dicMeterReadings.TryGetValue(kvp.Key, out var readingData))
                            continue;

                        var readingParts = readingData.Split(';');
                        int meterReading = int.Parse(readingParts[0]);
                        int meterFault = int.Parse(readingParts[1]);
                        int networkFault = int.Parse(kvp.Value);

                        using (SqlCommand insertCommand = new SqlCommand(
                            "INSERT INTO EtherMeterReadings (MeterID, NetworkFault, MeterFault, MeterReading, FlowRate, TimeStamp) " +
                            "VALUES (@MeterID, @NetworkFault, @MeterFault, @MeterReading, @FlowRate, @TimeStamp)", connection))
                        {
                            insertCommand.Parameters.AddWithValue("@MeterID", kvp.Key);
                            insertCommand.Parameters.AddWithValue("@NetworkFault", networkFault);
                            insertCommand.Parameters.AddWithValue("@MeterFault", meterFault);
                            insertCommand.Parameters.AddWithValue("@MeterReading", meterReading);
                            insertCommand.Parameters.AddWithValue("@FlowRate", 0);
                            insertCommand.Parameters.AddWithValue("@TimeStamp", DateTime.Now);

                            insertCommand.ExecuteNonQuery();
                        }

                        Log.Info($"    ✓ Inserted reading for MeterID {kvp.Key}: Reading={meterReading}, Fault={meterFault}, NetFault={networkFault}");
                    }

                    Log.Info("=== Starting Threshold Evaluation ===");

                    string strCurrentHour = "Hour" + (DateTime.Now.Hour == 0 ? 24 : DateTime.Now.Hour);

                    foreach (var kvp in Globals.dicMeterNetFaults)
                    {
                        if (!Globals.dicMeterReadings.TryGetValue(kvp.Key, out var readingData))
                            continue;

                        var readingParts = readingData.Split(';');
                        int currentReading = int.Parse(readingParts[0]);
                        int previousReading = 0;

                        using (SqlCommand previousCommand = new SqlCommand(
                            "SELECT TOP 1 MeterReading FROM EtherMeterReadings WHERE MeterID = @MeterID AND TimeStamp < @CurrentHour ORDER BY TimeStamp DESC", connection))
                        {
                            previousCommand.Parameters.AddWithValue("@MeterID", kvp.Key);
                            previousCommand.Parameters.AddWithValue("@CurrentHour", DateTime.Now.ToString("yyyy-MM-dd HH:00:00"));

                            using (SqlDataReader reader = previousCommand.ExecuteReader())
                            {
                                if (reader.Read() && !reader.IsDBNull(0))
                                {
                                    previousReading = Convert.ToInt32(reader.GetValue(0));
                                }
                            }
                        }

                        int usageDifference = currentReading - previousReading;

                        int threshold = 0;
                        using (SqlCommand thresholdCommand = new SqlCommand(
                            $"SELECT {strCurrentHour} FROM EtherMeterThresholds WHERE MeterID = @MeterID", connection))
                        {
                            thresholdCommand.Parameters.AddWithValue("@MeterID", kvp.Key);

                            using (SqlDataReader reader = thresholdCommand.ExecuteReader())
                            {
                                if (reader.Read() && int.TryParse(reader[0]?.ToString(), out threshold)) { }
                            }
                        }

                        if (threshold > 0)
                        {
                            Dictionary<string, float> adjustments = GetThresholdAdjustments(int.Parse(kvp.Key));
                            string adjustmentKey = GetCurrentAdjustmentKey();
                            if (adjustments.TryGetValue(adjustmentKey, out float adjustment))
                            {
                                threshold = (int)(threshold * adjustment);
                            }

                            float overagePercentage = (float)usageDifference / threshold;

                            Log.Info($"    MeterID {kvp.Key} - Threshold Report:");
                            Log.Info($"        Current Reading: {currentReading}");
                            Log.Info($"        Previous Reading: {previousReading}");
                            Log.Info($"        Usage Difference: {usageDifference}");
                            Log.Info($"        Adjusted Threshold ({adjustmentKey}): {threshold}");
                            Log.Info($"        Usage % of Threshold: {overagePercentage:P1}");

                            bool initialSent = Globals.dicInitialAlertSent.TryGetValue(kvp.Key, out bool initial) && initial;
                            bool secondarySent = Globals.dicSecondaryAlertSent.TryGetValue(kvp.Key, out bool secondary) && secondary;

                            if (int.TryParse(kvp.Key, out int meterID))
                            {
                                // Get the meter name from the dictionary
                                string meterName = Globals.MeterNames.ContainsKey(meterID) ? Globals.MeterNames[meterID] : "Unknown Meter";

                                if (overagePercentage > 1.0f && !initialSent)
                                {
                                    Log.Warn($"        ⚠️ Initial Threshold Breach Detected!");
                                    SendAlertEmail(meterID, $"⚠️ Initial threshold breach detected for {meterName} (MeterID {meterID}).\nUsage: {usageDifference}\nThreshold: {threshold}\nTime: {DateTime.Now}");
                                    Globals.dicInitialAlertSent[kvp.Key] = true;
                                }

                                if (overagePercentage > 1.10f && !secondarySent)
                                {
                                    Log.Warn($"        🚨 Secondary Threshold Breach Detected (110% Over)!");
                                    SendAlertEmail(meterID, $"🚨 Secondary alert: usage for {meterName} (MeterID {meterID}) exceeded 110% of the Threshold Limit.\nUsage: {usageDifference}\nThreshold: {threshold}\nTime: {DateTime.Now}");
                                    Globals.dicSecondaryAlertSent[kvp.Key] = true;
                                }
                            }
                            else
                            {
                                Log.Error($"Failed to parse MeterID from {kvp.Key}. Skipping alert.");
                            }

                        }
                        else
                        {
                            Log.Info($"    MeterID {kvp.Key} - No valid threshold found for current hour.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("❌ Error inserting readings or evaluating thresholds.", ex);
                return false;
            }
            finally
            {
                Globals.dicMeterIPs.Clear();
                Globals.dicMeterNetFaults.Clear();
                Globals.dicMeterReadings.Clear();
            }

            Log.Info("=== Reading Insert and Threshold Analysis Complete ===\n");

            return true;
        }


        private bool GetReadings()
        {
            Log.Info("=== Starting Meter Readings Collection ===");

            HtmlDocument doc = new HtmlDocument();
            using (WebClient client = new WebClient())
            {
                foreach (var kvp in Globals.dicMeterIPs)
                {
                    string meterID = kvp.Key;
                    string ipAddress = kvp.Value;
                    Globals.dicMeterNetFaults[meterID] = "0";

                    try
                    {
                        Log.Info($"    Fetching data from MeterID {meterID} at IP {ipAddress}...");

                        string strPageString = client.DownloadString($"http://{ipAddress}");
                        doc.LoadHtml(strPageString);

                        string reading = "", meterFault = "";
                        bool readingFound = false, faultFound = false;

                        foreach (HtmlNode table in doc.DocumentNode.SelectNodes("//table") ?? Enumerable.Empty<HtmlNode>())
                        {
                            foreach (HtmlNode row in table.SelectNodes("tr") ?? Enumerable.Empty<HtmlNode>())
                            {
                                foreach (HtmlNode cell in row.SelectNodes("td") ?? Enumerable.Empty<HtmlNode>())
                                {
                                    if (cell.InnerText == "Meter 1 Total")
                                    {
                                        readingFound = true;
                                    }
                                    else if (readingFound)
                                    {
                                        reading = cell.InnerText.Trim();
                                        readingFound = false;
                                    }

                                    if (cell.InnerText == "Meter 1 Fault")
                                    {
                                        faultFound = true;
                                    }
                                    else if (faultFound)
                                    {
                                        meterFault = cell.InnerText.Trim() == "NO" ? "0" : "1";
                                        faultFound = false;
                                    }
                                }
                            }
                        }

                        if (!string.IsNullOrEmpty(reading))
                        {
                            Globals.dicMeterReadings[meterID] = $"{reading};{meterFault}";
                            Log.Info($"        ✓ Reading: {reading}, Fault: {meterFault}");
                        }
                        else
                        {
                            Log.Warn($"        ⚠️ No reading found for MeterID {meterID}.");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"❌ Error fetching data from MeterID {meterID} (IP {ipAddress}).", ex);
                        Globals.dicMeterNetFaults[meterID] = "1";
                    }
                }
            }

            Log.Info("=== Meter Readings Collection Complete ===\n");

            return RecordNewReadings();
        }


        private bool GetReadingsFromMeters()
        {
            Log.Info("=== Starting Meter Profile Retrieval ===");

            string connectionString = Globals.sDBConnectString;
            string strSQL = "SELECT * FROM EtherMeterProfiles WHERE IsActive = 1 ORDER BY MeterID ASC";

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(strSQL, connection))
            {
                SqlDataReader rdr = null;
                try
                {
                    connection.Open();
                    rdr = command.ExecuteReader();

                    if (rdr.HasRows)
                    {
                        while (rdr.Read())
                        {
                            string meterID = rdr["MeterID"].ToString();
                            string ipAddress = rdr["IPAddress"].ToString();
                            Globals.dicMeterIPs[meterID] = ipAddress;
                            Log.Info($"    Retrieved MeterID {meterID} | IP {ipAddress}");
                        }
                    }
                    else
                    {
                        Log.Warn("⚠️ No active meter profiles found.");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("❌ Database error while retrieving meter profiles.", ex);
                    return false;
                }
                finally
                {
                    rdr?.Close();
                }
            }

            Log.Info("=== Meter Profile Retrieval Complete ===\n");

            return GetReadings();
        }


        protected override void OnStart(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            Log.Info("*********************************************************");
            Log.Info("The Sugarbush EtherMeter Reader Service has started.");
            ActionTimer.Start();
        }

        protected override void OnStop()
        {
            Log.Info("*********************************************************");
            Log.Info("The Sugarbush EtherMeter Reader Service has stopped.");
        }

        public static class Globals
        {
            public static string sDBConnectString = "Data Source=vm-sug-sql1;Initial Catalog=SBCorpInet;Persist Security Info=True;User ID=SBCorpInet;Password=SBCorpInet";
            public static DateTime dtLastReadingTime = new DateTime();
            public static Dictionary<string, string> dicMeterReadings = new Dictionary<string, string> { };
            public static Dictionary<string, string> dicMeterIPs = new Dictionary<string, string> { };
            public static Dictionary<string, string> dicMeterNetFaults = new Dictionary<string, string> { };
            public static Dictionary<string, bool> dicInitialAlertSent = new Dictionary<string, bool>();
            public static Dictionary<string, bool> dicSecondaryAlertSent = new Dictionary<string, bool>();

            public static Dictionary<int, string> MeterNames = new Dictionary<int, string>()
            {
                { 1, "Farmhouse" },
                { 2, "ValleyHouse" },
                { 3, "GateHouse" },
                { 4, "SchoolHouse" },
                { 5, "ClayBrook" }
            };


        }
    }
}
