using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;
using System.Reflection;
using System.Threading;
using System.Globalization;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace TKWEBPOSSYNC
{
    public partial class WSCMISYNC : Form
    {
        SqlConnection sqlConn = new SqlConnection();
        SqlCommand sqlComm = new SqlCommand();
        string connectionString;
        StringBuilder sbSql = new StringBuilder();
        SqlDataAdapter adapter = new SqlDataAdapter();
        SqlCommandBuilder sqlCmdBuilder = new SqlCommandBuilder();
        SqlTransaction tran;
        SqlCommand cmd = new SqlCommand();
        DataSet ds = new DataSet();
        int result;

        public WSCMISYNC()
        {
            InitializeComponent();
        }

        private void WSCMISYNC_Load(object sender, EventArgs e)
        {
            timer1.Interval = 1000; // 設定每秒觸發一次
            //timer1.Interval = 1000 * 60; // 設定每分觸發一次
            timer1.Enabled = true; // 啟動 Timer
        }

        #region
    

        //排程執行區-timer1_Tick 判斷

        private void timer1_Tick(object sender, EventArgs e)
        {
            label4.Text = DateTime.Now.ToString("yyyy/MM/dd hh:mm:ss");

            //INSERTLOGWSCMIBOUNS("GO");
            INSERTLOGLOGWSCMISYNC("GO");
        }

        public void INSERTLOGLOGWSCMISYNC(string message)
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();
                sbSql.AppendFormat(" INSERT [TKWEBPOSSYNC].[dbo].[LOGWSCMISYNC]");
                sbSql.AppendFormat(" ([EXECTIME],[STATUS])");
                sbSql.AppendFormat("  VALUES ('{0}','{1}')", label4.Text, message);
                sbSql.AppendFormat(" ");
                sbSql.AppendFormat(" ");

                cmd.Connection = sqlConn;
                cmd.CommandTimeout = 60;
                cmd.CommandText = sbSql.ToString();
                cmd.Transaction = tran;
                result = cmd.ExecuteNonQuery();

                if (result == 0)
                {
                    tran.Rollback();    //交易取消
                }
                else
                {
                    tran.Commit();      //執行交易  
                }

            }
            catch
            {

            }

            finally
            {
                sqlConn.Close();
            }
        }
        public void INSERTLOGWSCMIBOUNS(string message)
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();
                sbSql.AppendFormat(" INSERT [TKWEBPOSSYNC].[dbo].[LOGWSCMIBOUNS]");
                sbSql.AppendFormat(" ([EXECTIME],[STATUS])");
                sbSql.AppendFormat("  VALUES ('{0}','{1}')",label4.Text, message);
                sbSql.AppendFormat(" ");
                sbSql.AppendFormat(" ");

                cmd.Connection = sqlConn;
                cmd.CommandTimeout = 60;
                cmd.CommandText = sbSql.ToString();
                cmd.Transaction = tran;
                result = cmd.ExecuteNonQuery();

                if (result == 0)
                {
                    tran.Rollback();    //交易取消
                }
                else
                {
                    tran.Commit();      //執行交易  
                }

            }
            catch
            {

            }

            finally
            {
                sqlConn.Close();
            }
        }

        #endregion
    }
}
