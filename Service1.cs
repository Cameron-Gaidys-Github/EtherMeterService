using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using log4net;
using System.Data.SqlClient;
using System.Timers;
using System.Net;
using HtmlAgilityPack;
using System.Net.Mail;



namespace EtherMeterService
{
    public partial class Service1 : ServiceBase
    {
        private static readonly ILog Log =
            LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        Timer ActionTimer = new Timer();

        private Timer PingTestTimer = new Timer();

        private Timer LivePollTimer = new Timer();

        public Service1()
        {
            InitializeComponent();
            ActionTimer.Interval = 60000;
            ActionTimer.Enabled = false;
            ActionTimer.Elapsed += ActionTimer_Tick;

            LivePollTimer.Interval = 1000; // 1 second
            LivePollTimer.Enabled = false;
            LivePollTimer.Elapsed += LivePollTimer_Tick;

            PingTestTimer.Interval = 15 * 60 * 1000; // 15 minutes in milliseconds
            PingTestTimer.Enabled = false;
            PingTestTimer.Elapsed += PingTestTimer_Tick;
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

        private void PingTestTimer_Tick(object sender, EventArgs e)
        {
            foreach (var kvp in Globals.dicMeterIPs)
            {
                string meterID = kvp.Key;
                string ipAddress = kvp.Value;
                int intMeterID;
                bool isOnline = PingHost(ipAddress);

                if (int.TryParse(meterID, out intMeterID))
                {
                    bool wasOnline = Globals.MeterIsOnline.TryGetValue(intMeterID, out bool prevOnline) ? prevOnline : true;

                    if (!isOnline)
                    {
                        Log.Warn($"Ping test failed for MeterID {meterID} at IP {ipAddress}.");
                        string msg = $"Ping test failed for meter {meterID} ({ipAddress}) at {DateTime.Now}.";
                        SendOfflineAlertEmail(intMeterID, msg);
                        Globals.MeterIsOnline[intMeterID] = false;
                    }
                    else
                    {
                        Log.Info($"Ping test succeeded for MeterID {meterID} at IP {ipAddress}.");
                        if (!wasOnline)
                        {
                            // Meter has come back online
                            string meterName = Globals.MeterNames.ContainsKey(intMeterID) ? Globals.MeterNames[intMeterID] : "Unknown Meter";
                            string msg = $"Meter {meterName} (MeterID: {meterID}, IP: {ipAddress}) is back ONLINE at {DateTime.Now}.";
                            SendBackOnlineAlertEmail(intMeterID, msg);
                        }
                        Globals.MeterIsOnline[intMeterID] = true;
                    }
                }
            }
        }


        private bool PingHost(string ipAddress, int timeoutMs = 2000)
        {
            try
            {
                using (var ping = new System.Net.NetworkInformation.Ping())
                {
                    var reply = ping.Send(ipAddress, timeoutMs);
                    return reply.Status == System.Net.NetworkInformation.IPStatus.Success;
                }
            }
            catch
            {
                return false;
            }
        }

        private void LivePollTimer_Tick(object sender, EventArgs e)
        {
            try // NEW: keep the timer alive if anything throws
            {
                foreach (var kvp in Globals.dicMeterIPs)
                {
                    string meterID = kvp.Key;
                    string ipAddress = kvp.Value;

                    double currentFlow = GetLiveFlowFromMeter(ipAddress);
                    DateTime now = DateTime.Now;

                    if (!Globals.LiveFlowTracking.TryGetValue(meterID, out var tracking))
                    {
                        tracking = (DateTime.MinValue, 0);
                    }

                    if (currentFlow > 0)
                    {
                        // Flow detected
                        if (tracking.StartTime == DateTime.MinValue)
                        {
                            Globals.LiveFlowTracking[meterID] = (now, currentFlow);
                            Log.Info($"[LIVE] Started tracking sustained flow for Meter {meterID} at {currentFlow:F2} gpm");
                        }
                        else
                        {
                            double duration = (now - tracking.StartTime).TotalMinutes;
                            double dropFactor = 0.7;
                            double doubleFactor = 2.0;

                            if (currentFlow >= tracking.InitialFlow * doubleFactor)
                            {
                                Globals.LiveFlowTracking[meterID] = (now, currentFlow);
                                Log.Info($"[LIVE] Flow doubled for Meter {meterID}, resetting sustained tracking.");
                            }
                            else if (currentFlow < tracking.InitialFlow * dropFactor)
                            {
                                Globals.LiveFlowTracking.Remove(meterID);
                                if (Globals.LastSustainedFlowAlertSent.ContainsKey(meterID))
                                    Globals.LastSustainedFlowAlertSent.Remove(meterID);
                                Log.Info($"[LIVE] Flow dropped significantly for Meter {meterID}, resetting tracking.");
                            }
                            else
                            {
                                // Set custom duration for Meter 5 (ClayBrook)
                                double requiredDuration = (meterID == "5") ? 120.0 : 60.0;

                                if (duration >= requiredDuration)
                                {
                                    bool shouldSend = !Globals.LastSustainedFlowAlertSent.TryGetValue(meterID, out DateTime lastSent) ||
                                                      (now - lastSent).TotalMinutes >= requiredDuration;

                                    if (shouldSend)
                                    {
                                        if (int.TryParse(meterID, out int intMeterID))
                                        {
                                            string meterName = Globals.MeterNames.ContainsKey(intMeterID) ? Globals.MeterNames[intMeterID] : "Unknown Meter";
                                            string msg =
                                                $"[LIVE ALERT]\n" +
                                                $"Sustained flow detected for {meterName} (MeterID: {meterID})\n" +
                                                $"Flow: {currentFlow:F2} gal/min\n" +
                                                $"Duration: {duration:F1} minutes\n" +
                                                $"Start: {tracking.StartTime}\n" +
                                                $"Now: {now}";

                                            SendAlertEmail(intMeterID, msg);
                                            Globals.LastSustainedFlowAlertSent[meterID] = now;
                                            Log.Warn($"[LIVE] Sustained flow alert sent for Meter {meterID}. Next eligible after {requiredDuration} minutes.");
                                        }
                                    }
                                }
                            }
                        }

                        // Clear zero flow marker safely
                        if (Globals.LastZeroFlowTime.ContainsKey(meterID))
                            Globals.LastZeroFlowTime.Remove(meterID);
                    }
                    else
                    {
                        // Flow is zero
                        if (!Globals.LastZeroFlowTime.ContainsKey(meterID))
                        {
                            Globals.LastZeroFlowTime[meterID] = now;
                        }
                        else if ((now - Globals.LastZeroFlowTime[meterID]).TotalSeconds > 10)
                        {
                            Globals.LiveFlowTracking.Remove(meterID);
                            if (Globals.LastSustainedFlowAlertSent.ContainsKey(meterID))
                                Globals.LastSustainedFlowAlertSent.Remove(meterID);
                            Globals.LastZeroFlowTime.Remove(meterID);
                            Log.Info($"[LIVE] Zero flow sustained for Meter {meterID}, resetting tracking and alerts.");
                        }
                    }
                }

                // Fault detection (unchanged)
                foreach (var kvp in Globals.dicMeterNetFaults)
                {
                    string meterID = kvp.Key;
                    string netFault = kvp.Value;

                    if (netFault == "1")
                    {
                        if (int.TryParse(meterID, out int intMeterID))
                        {
                            string msg = $"Network fault detected for meter {meterID} at {DateTime.Now}.";
                            SendOfflineAlertEmail(intMeterID, msg);
                        }
                    }
                    else if (Globals.dicMeterReadings.TryGetValue(meterID, out var readingData))
                    {
                        var readingParts = readingData.Split(';');
                        if (readingParts.Length > 1 && readingParts[1] == "1")
                        {
                            if (int.TryParse(meterID, out int intMeterID))
                            {
                                string msg = $"Meter fault detected for meter {meterID} at {DateTime.Now}.";
                                SendOfflineAlertEmail(intMeterID, msg);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("Unexpected error during LivePollTimer_Tick.", ex);
            }
        }



        private double GetLiveFlowFromMeter(string ipAddress)
        {
            try
            {
                using (WebClient client = new WebClient())
                {
                    string url = $"http://{ipAddress}";
                    string pageContent = client.DownloadString(url);

                    HtmlDocument doc = new HtmlDocument();
                    doc.LoadHtml(pageContent);

                    foreach (HtmlNode table in doc.DocumentNode.SelectNodes("//table") ?? Enumerable.Empty<HtmlNode>())
                    {
                        foreach (HtmlNode row in table.SelectNodes("tr") ?? Enumerable.Empty<HtmlNode>())
                        {
                            var cells = row.SelectNodes("td")?.ToList();
                            if (cells == null || cells.Count < 2)
                                continue;

                            string label = cells[0].InnerText.Trim();
                            if (label.Equals("Meter 1 Flow", StringComparison.OrdinalIgnoreCase))
                            {
                                string valueText = cells[1].InnerText.Trim().Replace("+", "").Replace(",", "");
                                if (double.TryParse(valueText, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double flow))
                                    return flow;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Error fetching live flow from {ipAddress}: {ex}");
            }
            return 0.0;
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

        bool SendAlertEmail(int meterID, string emailMessage, IEnumerable<string> recipients = null)
        {
            try
            {
                string meterName = Globals.MeterNames.ContainsKey(meterID) ? Globals.MeterNames[meterID] : "Unknown Meter";
                emailMessage = $"Alert for {meterName} (MeterID: {meterID}):\n" + emailMessage;

                SmtpClient mySmtpClient = new SmtpClient("smtp-relay.idirectory.itw")
                {
                    Port = 25,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = true
                };

                MailMessage mail = new MailMessage
                {
                    From = new MailAddress("EthermeterAlerts@sugarbush.com"),
                    Subject = "Alert Notification",
                    Body = emailMessage,
                    IsBodyHtml = false
                };

                // Add recipients
                if (recipients != null && recipients.Any())
                {
                    foreach (var recipient in recipients)
                    {
                        if (!string.IsNullOrWhiteSpace(recipient))
                            mail.To.Add(recipient);
                    }
                }
                else
                {
                    mail.To.Add("Waterusagealerts@sugarbush.com");
                }

                mySmtpClient.Send(mail);
                Log.Info("Alert email sent successfully.");
            }
            catch (Exception ex)
            {
                Log.Error("Error sending alert email. " + ex.ToString());
            }

            return true;
        }

        private bool SendBackOnlineAlertEmail(int meterID, string emailMessage)
        {
            try
            {
                string meterName = Globals.MeterNames.ContainsKey(meterID) ? Globals.MeterNames[meterID] : "Unknown Meter";
                emailMessage = $"ONLINE ALERT for {meterName} (MeterID: {meterID}):\n" + emailMessage;

                SmtpClient mySmtpClient = new SmtpClient("smtp-relay.idirectory.itw")
                {
                    Port = 25,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = true
                };

                MailMessage mail = new MailMessage
                {
                    From = new MailAddress("EthermeterAlerts@sugarbush.com"),
                    Subject = "Meter Back Online Alert",
                    Body = emailMessage,
                    IsBodyHtml = false
                };

                // Use a dedicated recipient for online alerts
                mail.To.Add("italerts@sugarbush.com");

                mySmtpClient.Send(mail);
                Log.Info("Back online alert email sent successfully.");
            }
            catch (Exception ex)
            {
                Log.Error("Error sending back online alert email. " + ex.ToString());
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

        private bool SendOfflineAlertEmail(int meterID, string emailMessage)
        {
            try
            {
                // Check if an alert was sent within the last hour
                if (Globals.LastOfflineAlertSent.TryGetValue(meterID, out DateTime lastSent))
                {
                    if ((DateTime.Now - lastSent).TotalMinutes < 360)
                    {
                        Log.Info($"Offline alert for MeterID {meterID} suppressed (last sent at {lastSent}).");
                        return false;
                    }
                }

                string meterName = Globals.MeterNames.ContainsKey(meterID) ? Globals.MeterNames[meterID] : "Unknown Meter";
                emailMessage = $"OFFLINE ALERT for {meterName} (MeterID: {meterID}):\n" + emailMessage;

                SmtpClient mySmtpClient = new SmtpClient("smtp-relay.idirectory.itw")
                {
                    Port = 25,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = true
                };

                MailMessage mail = new MailMessage
                {
                    From = new MailAddress("EthermeterAlerts@sugarbush.com"),
                    Subject = "Meter Offline Alert",
                    Body = emailMessage,
                    IsBodyHtml = false
                };

                // Use a dedicated recipient for offline alerts
                mail.To.Add("italerts@sugarbush.com");

                mySmtpClient.Send(mail);
                Log.Info("Offline alert email sent successfully.");

                // Update last sent time
                Globals.LastOfflineAlertSent[meterID] = DateTime.Now;
            }
            catch (Exception ex)
            {
                Log.Error("Error sending offline alert email. " + ex.ToString());
            }

            return true;
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

                        int lowReading = int.Parse(readingParts[0]);
                        int lowFault = int.Parse(readingParts[1]);
                        int highReading = 0;
                        int highFault = 0;

                        if (kvp.Key == "5" && readingParts.Length >= 4)
                        {
                            highReading = int.Parse(readingParts[2]);
                            highFault = int.Parse(readingParts[3]);
                        }

                        int previousLow = 0;
                        int previousHigh = 0;

                        using (SqlCommand previousCommand = new SqlCommand(
                            "SELECT TOP 1 MeterReading, HighFlowReading FROM EtherMeterReadings WHERE MeterID = @MeterID AND TimeStamp < @CurrentHour ORDER BY TimeStamp DESC", connection))
                        {
                            previousCommand.Parameters.AddWithValue("@MeterID", kvp.Key);
                            previousCommand.Parameters.AddWithValue("@CurrentHour", DateTime.Now.ToString("yyyy-MM-dd HH:00:00"));

                            using (SqlDataReader reader = previousCommand.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    previousLow = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                                    if (kvp.Key == "5" && !reader.IsDBNull(1))
                                        previousHigh = Convert.ToInt32(reader.GetValue(1));
                                }
                            }
                        }

                        int usageDifference = lowReading - previousLow;

                        using (SqlCommand insertCommand = new SqlCommand(
                            "INSERT INTO EtherMeterReadings (MeterID, NetworkFault, MeterFault, MeterReading, HighFlowReading, FlowRate, TimeStamp) " +
                            "VALUES (@MeterID, @NetworkFault, @MeterFault, @MeterReading, @HighFlowReading, @FlowRate, @TimeStamp)", connection))
                        {
                            insertCommand.Parameters.AddWithValue("@MeterID", kvp.Key);
                            insertCommand.Parameters.AddWithValue("@NetworkFault", int.Parse(kvp.Value));
                            insertCommand.Parameters.AddWithValue("@MeterFault", lowFault);
                            insertCommand.Parameters.AddWithValue("@MeterReading", lowReading);
                            insertCommand.Parameters.AddWithValue("@HighFlowReading", highReading);
                            insertCommand.Parameters.AddWithValue("@FlowRate", usageDifference);
                            insertCommand.Parameters.AddWithValue("@TimeStamp", DateTime.Now);

                            insertCommand.ExecuteNonQuery();
                        }

                        Log.Info($"    ✓ Inserted reading for MeterID {kvp.Key}: Low={lowReading}, High={highReading}, FlowRate={usageDifference}");
                    }

                    // Threshold evaluation (no change — uses usageDifference)
                    Log.Info("=== Starting Threshold Evaluation ===");

                    string strCurrentHour = "Hour" + (DateTime.Now.Hour == 0 ? 24 : DateTime.Now.Hour);

                    foreach (var kvp in Globals.dicMeterNetFaults)
                    {
                        if (!Globals.dicMeterReadings.TryGetValue(kvp.Key, out var readingData))
                            continue;

                        var readingParts = readingData.Split(';');

                        int lowReading = int.Parse(readingParts[0]);
                        int highReading = (kvp.Key == "5" && readingParts.Length >= 4) ? int.Parse(readingParts[2]) : 0;

                        int previousLow = 0, previousHigh = 0;

                        using (SqlCommand previousCommand = new SqlCommand(
                            "SELECT TOP 1 MeterReading, HighFlowReading FROM EtherMeterReadings WHERE MeterID = @MeterID AND TimeStamp < @CurrentHour ORDER BY TimeStamp DESC", connection))
                        {
                            previousCommand.Parameters.AddWithValue("@MeterID", kvp.Key);
                            previousCommand.Parameters.AddWithValue("@CurrentHour", DateTime.Now.ToString("yyyy-MM-dd HH:00:00"));

                            using (SqlDataReader reader = previousCommand.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    previousLow = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                                    if (kvp.Key == "5" && !reader.IsDBNull(1))
                                        previousHigh = Convert.ToInt32(reader.GetValue(1));
                                }
                            }
                        }

                        int usageDifference = lowReading - previousLow;

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
                            Log.Info($"        Usage: {usageDifference}, Threshold: {threshold} ({adjustmentKey}), %: {overagePercentage:P1}");

                            bool initialSent = Globals.dicInitialAlertSent.TryGetValue(kvp.Key, out bool initial) && initial;
                            bool secondarySent = Globals.dicSecondaryAlertSent.TryGetValue(kvp.Key, out bool secondary) && secondary;

                            if (int.TryParse(kvp.Key, out int meterID))
                            {
                                string meterName = Globals.MeterNames.ContainsKey(meterID) ? Globals.MeterNames[meterID] : "Unknown Meter";

                                if (overagePercentage > 1.0f && !initialSent)
                                {
                                    Log.Warn($"        ⚠️ Initial Threshold Breach Detected!");
                                    SendAlertEmail(meterID, $"⚠️ Initial threshold breach detected for {meterName}.\nUsage: {usageDifference}\nThreshold: {threshold}\nTime: {DateTime.Now}");
                                    Globals.dicInitialAlertSent[kvp.Key] = true;
                                }

                                if (overagePercentage > 1.50f && !secondarySent)
                                {
                                    Log.Warn($"        🚨 Secondary Threshold Breach Detected!");
                                    SendAlertEmail(meterID, $"🚨 Secondary alert: usage for {meterName} exceeded 150% of threshold.\nUsage: {usageDifference}\nThreshold: {threshold}\nTime: {DateTime.Now}");
                                    Globals.dicSecondaryAlertSent[kvp.Key] = true;
                                }
                            }
                        }
                        else
                        {
                            Log.Info($"    MeterID {kvp.Key} - No valid threshold found.");
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
                //Globals.dicMeterIPs.Clear();
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

                        // Initialize
                        string lowReading = "", lowFault = "";
                        string highReading = "0", highFault = "0";
                        bool foundLowReading = false, foundLowFault = false;
                        bool foundHighReading = false, foundHighFault = false;

                        foreach (HtmlNode table in doc.DocumentNode.SelectNodes("//table") ?? Enumerable.Empty<HtmlNode>())
                            foreach (HtmlNode row in table.SelectNodes("tr") ?? Enumerable.Empty<HtmlNode>())
                                foreach (HtmlNode cell in row.SelectNodes("td") ?? Enumerable.Empty<HtmlNode>())
                                {
                                    string cleanText = cell.InnerText.Trim();

                                    // Meter 1 Total
                                    if (cleanText.IndexOf("Meter 1 Total", StringComparison.OrdinalIgnoreCase) >= 0)
                                        foundLowReading = true;
                                    else if (foundLowReading)
                                    {
                                        lowReading = cleanText;
                                        foundLowReading = false;
                                    }

                                    // Meter 1 Fault
                                    if (cleanText.IndexOf("Meter 1 Fault", StringComparison.OrdinalIgnoreCase) >= 0)
                                        foundLowFault = true;
                                    else if (foundLowFault)
                                    {
                                        lowFault = (cleanText.Equals("NO", StringComparison.OrdinalIgnoreCase) ? "0" : "1");
                                        foundLowFault = false;
                                    }

                                    // Only for Meter 5: Meter 2 Total / Fault
                                    if (meterID == "5")
                                    {
                                        if (cleanText.IndexOf("Meter 2 Total", StringComparison.OrdinalIgnoreCase) >= 0)
                                            foundHighReading = true;
                                        else if (foundHighReading)
                                        {
                                            highReading = cleanText;
                                            foundHighReading = false;
                                        }

                                        if (cleanText.IndexOf("Meter 2 Fault", StringComparison.OrdinalIgnoreCase) >= 0)
                                            foundHighFault = true;
                                        else if (foundHighFault)
                                        {
                                            highFault = (cleanText.Equals("NO", StringComparison.OrdinalIgnoreCase) ? "0" : "1");
                                            foundHighFault = false;
                                        }
                                    }
                                }

                        if (!string.IsNullOrEmpty(lowReading))
                        {
                            Globals.dicMeterReadings[meterID] = meterID == "5"
                                ? $"{lowReading};{lowFault};{highReading};{highFault}"
                                : $"{lowReading};{lowFault}";

                            Log.Info($"        ✓ Reading for MeterID {meterID}: Low={lowReading}, High={highReading}, Faults L:{lowFault} H:{highFault}");
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
                if (Globals.dicMeterReadings.Count == 0)
                {
                    Log.Warn("No readings could be obtained from any meter. Sending offline alert.");
                    SendOfflineAlertEmail(0, "No readings could be obtained from any meter at " + DateTime.Now);
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
            LivePollTimer.Start();
            PingTestTimer.Start();
        }

        protected override void OnStop()
        {
            Log.Info("*********************************************************");
            Log.Info("The Sugarbush EtherMeter Reader Service has stopped.");
            ActionTimer.Stop();
            LivePollTimer.Stop();
            PingTestTimer.Stop();
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
            public static Dictionary<string, Queue<(DateTime Timestamp, double Flow)>> LiveFlowWindows = new Dictionary<string, Queue<(DateTime, double)>>();
            public static Dictionary<string, (DateTime StartTime, double InitialFlow)> LiveFlowTracking = new Dictionary<string, (DateTime, double)>();
            public static Dictionary<int, DateTime> LastOfflineAlertSent = new Dictionary<int, DateTime>();
            public static Dictionary<int, bool> MeterIsOnline = new Dictionary<int, bool>();
            public static Dictionary<string, DateTime> LastZeroFlowTime = new Dictionary<string, DateTime>();
            public static Dictionary<string, DateTime> LastSustainedFlowAlertSent = new Dictionary<string, DateTime>();





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
