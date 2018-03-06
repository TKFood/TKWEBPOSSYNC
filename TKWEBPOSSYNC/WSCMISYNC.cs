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
        DataSet dsMYSQLWSCMISYNCUPDATE = new DataSet();
        DataSet dsMSSQLWSCMI = new DataSet();

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
                //UPDATEMYSQLWSCMISYNC();
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


        public void UPDATEMYSQLWSCMISYNC()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.AppendFormat(@" SELECT  A.MI001, A.EMAIL, A.NAME, A.PHONE, A.ADDRESS, A.TEL, CONVERT(varchar(100),A.BIRTHDAY,111) AS BIRTHDAY, A.PASSWORD, A.SEX, A.FORM, A.STATUS ");
                sbSql.AppendFormat(@" FROM [TKWEBPOSSYNC].[dbo].[WSCMISYNC] A ");
                sbSql.AppendFormat(@" INNER JOIN OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI') B  ");
                sbSql.AppendFormat(@" ON A.MI001=B.MI001 AND (A.EMAIL<>B.EMAIL OR A.NAME<>B.NAME OR A.PHONE<>B.PHONE OR A.ADDRESS<>B.ADDRESS OR A.TEL<>B.TEL OR A.BIRTHDAY<>B.BIRTHDAY OR A.SEX<>B.SEX )");
                sbSql.AppendFormat(@" AND A.STATUS='N'");
                sbSql.AppendFormat(@" ");
                sbSql.AppendFormat(@" ");

                adapter = new SqlDataAdapter(sbSql.ToString(), sqlConn);
                sqlCmdBuilder = new SqlCommandBuilder(adapter);

                sqlConn.Open();
                dsMYSQLWSCMISYNCUPDATE.Clear();
                //dataGridView1.Columns.Clear();


                adapter.Fill(dsMYSQLWSCMISYNCUPDATE, "MYSQLWSCMISYNCUPDATE");
                sqlConn.Close();

                if (dsMYSQLWSCMISYNCUPDATE.Tables["MYSQLWSCMISYNCUPDATE"].Rows.Count == 0)
                {

                }
                else if (dsMYSQLWSCMISYNCUPDATE.Tables["MYSQLWSCMISYNCUPDATE"].Rows.Count >= 1)
                {
                    UPDATETOMYSQLWSCMISYNC();
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

        public void UPDATETOMYSQLWSCMISYNC()
        {
            string connString = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;

            MySqlConnection conn = new MySqlConnection(connString);
            conn.Open();

            MySqlCommand UPDATENewCmd;
            StringBuilder UPDATENew = new StringBuilder();

            foreach (DataRow od in dsMYSQLWSCMISYNCUPDATE.Tables["MYSQLWSCMISYNCUPDATE"].Rows)
            {
                UPDATENew.AppendFormat(@" UPDATE NEWDB.WSCMI SET EMAIL='{1}',NAME='{2}',PHONE='{3}',ADDRESS='{4}',TEL='{5}',BIRTHDAY='{6}',PASSWORD='{7}',SEX='{8}' WHERE MI001='{0}' ", od["MI001"].ToString(), od["EMAIL"].ToString(), od["NAME"].ToString(), od["PHONE"].ToString(), od["ADDRESS"].ToString(), od["TEL"].ToString(), od["BIRTHDAY"].ToString(), od["PASSWORD"].ToString(), od["SEX"].ToString());
                UPDATENew.AppendFormat(@" ");

            }

            UPDATENewCmd = new MySqlCommand(UPDATENew.ToString(), conn);
            UPDATENewCmd.Connection = conn;
            //執行新增
            UPDATENewCmd.ExecuteNonQuery();
        }


        public void INSERTTOMSSQLWSCMI()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.Clear();

                sbSql.AppendFormat(@" SELECT  * FROM  OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI')");
                sbSql.AppendFormat(@" WHERE MI001 NOT IN (SELECT [MI001] FROM [test].[dbo].[WSCMI])");
                sbSql.AppendFormat(@" ");

                adapter = new SqlDataAdapter(sbSql.ToString(), sqlConn);
                sqlCmdBuilder = new SqlCommandBuilder(adapter);

                sqlConn.Open();
                dsMSSQLWSCMI.Clear();
                //dataGridView1.Columns.Clear();


                adapter.Fill(dsMSSQLWSCMI, "MSSQLWSCMI");
                sqlConn.Close();

                if (dsMSSQLWSCMI.Tables["MSSQLWSCMI"].Rows.Count == 0)
                {

                }
                else if (dsMSSQLWSCMI.Tables["MSSQLWSCMI"].Rows.Count >= 1)
                {
                    ADDTOMSSQLQSCMI();
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

        public void ADDTOMSSQLQSCMI()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();


                sbSql.AppendFormat(" INSERT INTO [test].[dbo].[WSCMI]");
                sbSql.AppendFormat(" ([COMPANY],[CREATOR],[USR_GROUP],[CREATE_DATE],[MODIFIER],[MODI_DATE],[FLAG],[CREATE_TIME],[MODI_TIME],[TRANS_TYPE]");
                sbSql.AppendFormat(" ,[TRANS_NAME],[sync_date],[sync_time],[sync_mark],[sync_count],[DataUser],[DataGroup],[MI001],[MI002],[MI003]");
                sbSql.AppendFormat(" ,[MI004],[MI005],[MI006],[MI007],[MI008],[MI009],[MI010],[MI011],[MI012],[MI013]");
                sbSql.AppendFormat(" ,[MI014],[MI015],[MI016],[MI017],[MI018],[MI019],[MI020],[MI021],[MI022],[MI023]");
                sbSql.AppendFormat(" ,[MI024],[MI025],[MI026],[MI027],[MI028],[MI029],[MI030],[MI031],[MI032],[MI033]");
                sbSql.AppendFormat(" ,[MI034],[MI035],[MI036],[MI037],[MI038],[MI039],[MI040],[MI041],[MI042],[MI043]");
                sbSql.AppendFormat(" ,[MI044],[MI045],[MI046],[MI047],[MI048],[MI049],[MI050],[MI051],[MI052],[MI053]");
                sbSql.AppendFormat(" ,[MI054],[MI055],[MI056],[MI057],[MI058],[MI059],[MI060],[MI061],[MI062],[MI063]");
                sbSql.AppendFormat(" ,[MI064],[MI065],[MI066],[MI067],[MI068],[MI069],[MI070],[MI071],[MI072],[MI073]");
                sbSql.AppendFormat(" ,[MI074],[MI075],[UDF01],[UDF02],[UDF03],[UDF04],[UDF05],[UDF06],[UDF07],[UDF08]");
                sbSql.AppendFormat(" ,[UDF09],[UDF10])");
                sbSql.AppendFormat(" ");
                sbSql.AppendFormat(" SELECT 'TK' AS [COMPANY],'DS' AS [CREATOR],'DS' AS [USR_GROUP],convert(varchar, getdate(), 112) AS [CREATE_DATE],'DS' AS [MODIFIER],convert(varchar, getdate(), 112) AS [MODI_DATE],1 AS [FLAG],convert(varchar, getdate(), 108) AS [CREATE_TIME],convert(varchar, getdate(), 108) AS [MODI_TIME],'P001' AS [TRANS_TYPE]");
                sbSql.AppendFormat(" ,'POSI13' AS [TRANS_NAME],NULL AS [sync_date],NULL AS [sync_time],NULL AS [sync_mark],0 AS [sync_count],NULL AS [DataUser],'DS' AS [DataGroup],A.MI001 AS [MI001],A.NAME AS [MI002],A.ADDRESS AS [MI003]");
                sbSql.AppendFormat(" ,A.TEL AS [MI004],convert(varchar, A.BIRTHDAY, 112) AS [MI005],convert(varchar, getdate(), 112) AS [MI006],NULL AS [MI007],NULL AS [MI008],NULL AS [MI009],A.SEX AS [MI010],NULL AS [MI011],0 AS [MI012],0 AS [MI013]");
                sbSql.AppendFormat(" ,NULL AS [MI014],NULL AS [MI015],NULL AS [MI016],NULL AS [MI017],A.MI001 AS [MI018],NULL AS [MI019],'9' AS [MI020],'3' AS [MI021],'2' AS [MI022],'1' AS [MI023]");
                sbSql.AppendFormat(" ,NULL AS [MI024],NULL AS [MI025],'2' AS [MI026],0 AS [MI027],0 AS [MI028],A.PHONE AS [MI029],NULL AS [MI030],NULL AS [MI031],NULL AS [MI032],NULL AS [MI033]");
                sbSql.AppendFormat(" ,0 AS [MI034],0 AS [MI035],0 AS [MI036],0 AS [MI037],NULL AS [MI038],NULL AS [MI039],NULL AS [MI040],NULL AS [MI041],NULL AS [MI042],NULL AS [MI043]");
                sbSql.AppendFormat(" ,NULL AS [MI044],NULL AS [MI045],NULL AS [MI046],NULL AS [MI047],NULL AS [MI048],NULL AS [MI049],NULL AS [MI050],NULL AS [MI051],NULL AS [MI052],NULL AS [MI053]");
                sbSql.AppendFormat(" ,NULL AS [MI054],NULL AS [MI055],NULL AS [MI056],NULL AS [MI057],NULL AS [MI058],NULL AS [MI059],NULL AS [MI060],0 AS [MI061],'N' AS [MI062],NULL AS [MI063]");
                sbSql.AppendFormat(" ,NULL AS [MI064],NULL AS [MI065],NULL AS [MI066],0 AS [MI067],0 AS [MI068],NULL AS [MI069],0 AS [MI070],0 AS [MI071],0 AS [MI072],0 AS [MI073]");
                sbSql.AppendFormat(" ,NULL AS [MI074],0 AS [MI075],NULL AS [UDF01],NULL AS [UDF02],NULL AS [UDF03],NULL AS [UDF04],NULL AS [UDF05],0 AS [UDF06],0 AS [UDF07],0 AS [UDF08]");
                sbSql.AppendFormat(" ,0 AS [UDF09],0 AS [UDF10]");
                sbSql.AppendFormat(" FROM  OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI') A");
                sbSql.AppendFormat(" WHERE A.MI001 NOT IN (SELECT [MI001] FROM [test].[dbo].[WSCMI])");
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

        #endregion


        #region
        private void button2_Click(object sender, EventArgs e)
        {
            //INSERTTOMYSQLWSCMISYNC();
            // UPDATEMYSQLWSCMISYNC();
            INSERTTOMSSQLWSCMI();
        }
        #endregion


    }
}
