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

        Dictionary<string, int> GetThresholdAdjustments(int meterID)
        {
            Dictionary<string, int> adjustments = new Dictionary<string, int>();
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
                        adjustments["Christmas Week"] = rdr["Christmas Week"] != DBNull.Value ? Convert.ToInt32(rdr["Christmas Week"]) : 0;
                        adjustments["MLK Week"] = rdr["MLK Week"] != DBNull.Value ? Convert.ToInt32(rdr["MLK Week"]) : 0;
                        adjustments["Presidents Week"] = rdr["Presidents Week"] != DBNull.Value ? Convert.ToInt32(rdr["Presidents Week"]) : 0;
                        adjustments["January"] = rdr["January"] != DBNull.Value ? Convert.ToInt32(rdr["January"]) : 0;
                        adjustments["February"] = rdr["February"] != DBNull.Value ? Convert.ToInt32(rdr["February"]) : 0;
                        adjustments["March"] = rdr["March"] != DBNull.Value ? Convert.ToInt32(rdr["March"]) : 0;
                        adjustments["April"] = rdr["April"] != DBNull.Value ? Convert.ToInt32(rdr["April"]) : 0;
                        adjustments["May"] = rdr["May"] != DBNull.Value ? Convert.ToInt32(rdr["May"]) : 0;
                        adjustments["June"] = rdr["June"] != DBNull.Value ? Convert.ToInt32(rdr["June"]) : 0;
                        adjustments["July"] = rdr["July"] != DBNull.Value ? Convert.ToInt32(rdr["July"]) : 0;
                        adjustments["August"] = rdr["August"] != DBNull.Value ? Convert.ToInt32(rdr["August"]) : 0;
                        adjustments["September"] = rdr["September"] != DBNull.Value ? Convert.ToInt32(rdr["September"]) : 0;
                        adjustments["October"] = rdr["October"] != DBNull.Value ? Convert.ToInt32(rdr["October"]) : 0;
                        adjustments["November"] = rdr["November"] != DBNull.Value ? Convert.ToInt32(rdr["November"]) : 0;
                        adjustments["December"] = rdr["December"] != DBNull.Value ? Convert.ToInt32(rdr["December"]) : 0;

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

        bool SendAlertEmail(string emailMessage)
        {
            try
            {
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
                    From = new MailAddress("EthermeterAlerts@sugarbush.com"), // Replace with a valid sender email
                    Subject = "Alert Notification",
                    Body = emailMessage,
                    IsBodyHtml = false
                };

                // Add recipient(s)
                mail.To.Add("CGaidys@sugarbush.com"); // Replace with the actual recipient email

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

        bool RecordNewReadings()
        {
            string connectionString = Globals.sDBConnectString;
            SqlConnection connection = new SqlConnection(connectionString);
            string strSQL = "";
            SqlCommand command;
            SqlDataReader rdr = null;

            Log.Info("Inserting the latest readings into the database.");

            try
            {
                connection.Open();
                command = new SqlCommand(strSQL, connection);

                foreach (var kvp in Globals.dicMeterNetFaults)
                {
                    if (Globals.dicMeterReadings.ContainsKey(kvp.Key))
                    {
                        Log.Info($"Inserting reading for MeterID: {kvp.Key}");
                        command.CommandText = "insert into EtherMeterReadings(MeterID, NetworkFault, MeterFault, MeterReading, FlowRate, TimeStamp) " +
                                              "values('" + kvp.Key + "', " + kvp.Value + ", " + Globals.dicMeterReadings[kvp.Key].Split(';')[1] + ", " +
                                              Globals.dicMeterReadings[kvp.Key].Split(';')[0] + ", 0, '" + DateTime.Now.ToString() + "')";
                        command.ExecuteNonQuery();
                    }
                }

                Log.Info("Analyzing thresholds for breaches.");
                string strCurrentHour = "Hour" + (DateTime.Now.Hour == 0 ? 24 : DateTime.Now.Hour);

                foreach (var kvp in Globals.dicMeterNetFaults)
                {
                    if (Globals.dicMeterReadings.ContainsKey(kvp.Key))
                    {
                        Log.Info($"Checking threshold for MeterID: {kvp.Key}");

                        strSQL = "SELECT TOP 1 MeterReading FROM EtherMeterReadings " +
                                 "WHERE MeterID = '" + kvp.Key + "' AND TimeStamp < '" + DateTime.Now.ToString("yyyy-MM-dd HH:00:00") + "' " +
                                 "ORDER BY TimeStamp DESC";
                        command.CommandText = strSQL;
                        rdr = command.ExecuteReader();

                        int previousReading = 0;
                        if (rdr.HasRows)
                        {
                            rdr.Read();
                            previousReading = Convert.ToInt32(rdr["MeterReading"]);
                            Log.Info($"Previous reading for MeterID {kvp.Key}: {previousReading}");
                        }

                        rdr.Close();

                        int currentReading = Convert.ToInt32(Globals.dicMeterReadings[kvp.Key].Split(';')[0]);
                        int usageDifference = currentReading - previousReading;
                        Log.Info($"Current reading for MeterID {kvp.Key}: {currentReading}, Usage difference: {usageDifference}");

                        strSQL = $"SELECT {strCurrentHour} FROM EtherMeterThresholds WHERE MeterID = '{kvp.Key}'";
                        command.CommandText = strSQL;
                        rdr = command.ExecuteReader();

                        if (rdr.HasRows)
                        {
                            rdr.Read();
                            string thresholdValue = rdr[strCurrentHour].ToString();
                            rdr.Close();

                            if (thresholdValue != "NO THRESHOLD" && int.TryParse(thresholdValue, out int threshold))
                            {
                                Log.Info($"Base threshold for MeterID {kvp.Key}, Hour {strCurrentHour}: {threshold}");

                                Dictionary<string, int> adjustments = GetThresholdAdjustments(int.Parse(kvp.Key));
                                string adjustmentKey = GetCurrentAdjustmentKey();

                                if (adjustments.ContainsKey(adjustmentKey))
                                {
                                    int adjustment = adjustments[adjustmentKey];
                                    threshold += adjustment;
                                    Log.Info($"Adjusted threshold for MeterID {kvp.Key}, Key {adjustmentKey}: {threshold} (Adjustment: {adjustment})");
                                }

                                if (usageDifference > threshold)
                                {
                                    Log.Warn($"Threshold breach detected for MeterID {kvp.Key}. Usage: {usageDifference}, Threshold: {threshold}");

                                    // Send email alert
                                    string emailMessage = $"Threshold breach detected for MeterID {kvp.Key}.\n" +
                                                          $"Usage: {usageDifference}\n" +
                                                          $"Threshold: {threshold}\n" +
                                                          $"Time: {DateTime.Now}";
                                    SendAlertEmail(emailMessage);
                                }
                                else
                                {
                                    Log.Info($"No threshold breach for MeterID {kvp.Key}. Usage: {usageDifference}, Threshold: {threshold}");
                                }
                            }
                        }
                        else
                        {
                            Log.Info($"No threshold found for MeterID {kvp.Key} for {strCurrentHour}.");
                        }
                        rdr.Close();
                    }
                }

                command.Dispose();
                connection.Close();
            }
            catch (Exception ex)
            {
                Log.Error("Error inserting readings into the database. " + ex);
            }
            finally
            {
                if (connection != null)
                {
                    connection.Close();
                }
            }

            Globals.dicMeterIPs.Clear();
            Globals.dicMeterNetFaults.Clear();
            Globals.dicMeterReadings.Clear();

            return true;
        }

        bool GetReadings()
        {
            Log.Info("Iterate through the list of meters and get the readings from each one.");

            string strPageString, strReading = "", strNetfault = "", strMeterFault = "";

            bool bReadingFound = false;
            bool bReadingTaken = false;
            bool bFaultFound = false;
            bool bFaultTaken = false;
            bool bMeterDone = false;

            HtmlDocument doc = new HtmlDocument();

            using (WebClient client = new WebClient())
            {
                foreach (var kvp in Globals.dicMeterIPs)
                {
                    Globals.dicMeterNetFaults.Add(kvp.Key, "0");
                    try
                    {
                        strPageString = client.DownloadString("http://" + kvp.Value);
                        doc.LoadHtml(strPageString);
                        foreach (HtmlNode table in doc.DocumentNode.SelectNodes("//table"))
                        {
                            foreach (HtmlNode row in table.SelectNodes("tr"))
                            {
                                if (bMeterDone == true)
                                {
                                    Log.Info($"Finished reading MeterID: {kvp.Key}. Reading: {strReading}, Fault: {strMeterFault}");
                                    strMeterFault = strMeterFault.Trim() == "NO" ? "0" : "1";
                                    Globals.dicMeterReadings.Add(kvp.Key, $"{strReading};{strMeterFault}");
                                    bMeterDone = false;
                                    bReadingTaken = false;
                                    bReadingFound = false;
                                    bFaultFound = false;
                                    bFaultTaken = false;
                                    break;
                                }

                                foreach (HtmlNode cell in row.SelectNodes("td"))
                                {
                                    if (cell.InnerText == "Meter 1 Total")
                                    {
                                        bReadingFound = true;
                                    }
                                    else if (bReadingFound)
                                    {
                                        strReading = cell.InnerText;
                                        bReadingTaken = true;
                                        bReadingFound = false;
                                    }

                                    if (cell.InnerText == "Meter 1 Fault")
                                    {
                                        bFaultFound = true;
                                    }
                                    else if (bFaultFound)
                                    {
                                        strMeterFault = cell.InnerText;
                                        bFaultTaken = true;
                                        bMeterDone = true;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"Error getting data from MeterID: {kvp.Key}, IP: {kvp.Value}. Exception: {ex}");
                        Globals.dicMeterNetFaults[kvp.Key] = "1";
                    }
                }
            }

            Log.Info("Finished iterating through the list of meters.");
            // Call RecordNewReadings after readings are retrieved
            if (RecordNewReadings() == false)
            {
                Log.Error("RecordNewReadings failed.");
            }
            return true;
        }

        bool GetReadingsFromMeters()
        {
            Log.Info("Retrieving list of active meter profiles from the database.");

            string connectionString = Globals.sDBConnectString;
            SqlConnection connection = new SqlConnection(connectionString);
            string strSQL = "select * from EtherMeterProfiles where IsActive = 1 order by MeterID asc";
            SqlCommand command;
            SqlDataReader rdr = null;

            try
            {
                connection.Open();
                command = new SqlCommand(strSQL, connection);
                rdr = command.ExecuteReader();

                if (rdr.HasRows)
                {
                    while (rdr.Read())
                    {
                        string meterID = rdr["MeterID"].ToString();
                        string ipAddress = rdr["IPAddress"].ToString();
                        Globals.dicMeterIPs.Add(meterID, ipAddress);
                        Log.Info($"Retrieved MeterID: {meterID}, IP: {ipAddress}");
                    }
                }
                else
                {
                    Log.Info("No active meter profiles found in the database.");
                }
                rdr.Close();
                connection.Close();
            }
            catch (Exception ex)
            {
                Log.Error("Error retrieving meter profiles from the database. " + ex);
                return false;
            }
            finally
            {
                if (rdr != null)
                {
                    rdr.Close();
                }
                if (connection != null)
                {
                    connection.Close();
                }
            }

            Log.Info("Finished retrieving meter profiles.");
            GetReadings();

            return true;
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
        }
    }
}
