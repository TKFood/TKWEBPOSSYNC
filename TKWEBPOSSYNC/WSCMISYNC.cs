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

        DataSet dsMYSQLWSCMISYNC = new DataSet();
        int result;

        string SYNC = "N";

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
            if(SYNC.Equals("Y"))
            {
                label4.Text = DateTime.Now.ToString("yyyy/MM/dd hh:mm:ss");

                //INSERTTOMYSQLWSCMISYNC();
                //INSERTLOGWSCMIBOUNS("GO");
                //INSERTLOGLOGWSCMISYNC("GO");
            }


        }

        public void INSERTTOMYSQLWSCMISYNC()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.AppendFormat(@" SELECT [MI001],[EMAIL],[NAME],[PHONE],[ADDRESS],[TEL],CONVERT(varchar(100),[BIRTHDAY],111) AS BIRTHDAY,[PASSWORD],[SEX],[FORM],[STATUS] FROM [TKWEBPOSSYNC].[dbo].[WSCMISYNC]");
                sbSql.AppendFormat(@" WHERE [STATUS]='N'");
                sbSql.AppendFormat(@" AND [MI001] NOT IN (SELECT  [MI001] FROM  OPENQUERY(MYSQL, 'SELECT MI001 FROM NEWDB.WSCMI'))");
                sbSql.AppendFormat(@" ");
                sbSql.AppendFormat(@" ");

                adapter = new SqlDataAdapter(sbSql.ToString(), sqlConn);
                sqlCmdBuilder = new SqlCommandBuilder(adapter);

                sqlConn.Open();
                dsMYSQLWSCMISYNC.Clear();
                //dataGridView1.Columns.Clear();


                adapter.Fill(dsMYSQLWSCMISYNC, "MYSQLWSCMISYNC");
                sqlConn.Close();

                if (dsMYSQLWSCMISYNC.Tables["MYSQLWSCMISYNC"].Rows.Count == 0)
                {

                }
                else if (dsMYSQLWSCMISYNC.Tables["MYSQLWSCMISYNC"].Rows.Count >= 1)
                {
                    ADDTOMYSQLWSCMISYNC();
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

        public void ADDTOMYSQLWSCMISYNC()
        {
            string connString = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;

            MySqlConnection conn = new MySqlConnection(connString);
            conn.Open();

            MySqlCommand AddNewCmd;
            StringBuilder AddNew = new StringBuilder();

            foreach (DataRow od in dsMYSQLWSCMISYNC.Tables["MYSQLWSCMISYNC"].Rows)
            {
                AddNew.AppendFormat(@" INSERT INTO NEWDB.WSCMI(MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS) VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}'); ", od["MI001"].ToString(), od["EMAIL"].ToString(), od["NAME"].ToString(), od["PHONE"].ToString(), od["ADDRESS"].ToString(), od["TEL"].ToString(), od["BIRTHDAY"].ToString(), od["PASSWORD"].ToString(), od["SEX"].ToString(), od["FORM"].ToString(), od["STATUS"].ToString());
                AddNew.AppendFormat(@" ");

            }

            AddNewCmd = new MySqlCommand(AddNew.ToString(), conn);
            AddNewCmd.Connection = conn;
            //執行新增
            AddNewCmd.ExecuteNonQuery();
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
        private void button1_Click(object sender, EventArgs e)
        {
            if(button1.Text.Equals("啟動"))
            {
                button1.Text = "停止";
                SYNC = "Y";
            }
            else
            {
                button1.Text = "啟動";
                SYNC = "N";
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            INSERTTOMYSQLWSCMISYNC();
        }
        #endregion


    }
}
