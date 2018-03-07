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
        DataSet dsMYSQLWSCMIBOUNS = new DataSet();
        DataSet dsMSSQLWSCMIBOUNS = new DataSet();
        

        int result;

        string SYNC = "N";

        public WSCMISYNC()
        {
            InitializeComponent();
        }

        private void WSCMISYNC_Load(object sender, EventArgs e)
        {
            timer1.Interval = 1000*60; // 設定每秒觸發一次*60
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

                //執行順序不可改，會造成點數重覆寫到WEB、POS
                INSERTTOMSSQLWSCMI();
                UPDATETOMSSQLWSCMI();

                INSERTTOMSSQLWSCMIBOUNS();
                INSERTTOMYSQLWSCMIBOUNS();

                INSERTTOMYSQLWSCMISYNC();
                UPDATEMYSQLWSCMISYNC();
            }


        }

        private void timer2_Tick(object sender, EventArgs e)
        {

        }

        public void INSERTTOMYSQLWSCMISYNC()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.Clear();
                sbSql.AppendFormat(@" SELECT [MI001],[EMAIL],[NAME],[PHONE],[ADDRESS],[TEL],CONVERT(varchar(100),[BIRTHDAY],111) AS BIRTHDAY,[PASSWORD],[SEX],[FORM],[STATUS] FROM [TKWEBPOSSYNC].[dbo].[WSCMISYNC]");
                sbSql.AppendFormat(@" WHERE [STATUS]='N' AND FORM='POS'");
                sbSql.AppendFormat(@" AND [MI001] NOT IN (SELECT  [MI001] FROM  OPENQUERY(MYSQL, 'SELECT MI001 FROM NEWDB.WSCMI WHERE FORM=''POS'''))  ");
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
                    INSERTLOGLOGWSCMISYNC("ADDTOMYSQLWSCMISYNC","RUN");
                }
            }
            catch
            {
                INSERTLOGLOGWSCMISYNC("ADDTOMYSQLWSCMISYNC", "FAIL");
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

                sbSql.Clear();
                sbSql.AppendFormat(@" SELECT  A.MI001, A.EMAIL, A.NAME, A.PHONE, A.ADDRESS, A.TEL, CONVERT(varchar(100),A.BIRTHDAY,111) AS BIRTHDAY, A.PASSWORD, A.SEX, A.FORM, A.STATUS ");
                sbSql.AppendFormat(@" FROM [TKWEBPOSSYNC].[dbo].[WSCMISYNC] A ");
                sbSql.AppendFormat(@" INNER JOIN OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI') B  ");
                sbSql.AppendFormat(@" ON A.MI001=B.MI001 AND (A.EMAIL<>B.EMAIL OR A.NAME<>B.NAME OR A.PHONE<>B.PHONE OR A.ADDRESS<>B.ADDRESS OR A.TEL<>B.TEL OR A.BIRTHDAY<>B.BIRTHDAY OR A.SEX<>B.SEX OR A.FORM<>B.FORM )");
                sbSql.AppendFormat(@" AND A.STATUS='N' AND A.FORM='POS'");
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
                    INSERTLOGLOGWSCMISYNC("UPDATETOMYSQLWSCMISYNC", "RUN");
                }
            }
            catch
            {
                INSERTLOGLOGWSCMISYNC("UPDATETOMYSQLWSCMISYNC", "FAIL");
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
                UPDATENew.AppendFormat(@" UPDATE NEWDB.WSCMI SET EMAIL='{1}',NAME='{2}',PHONE='{3}',ADDRESS='{4}',TEL='{5}',BIRTHDAY='{6}',PASSWORD='{7}',SEX='{8}' WHERE MI001='{0}' ;", od["MI001"].ToString(), od["EMAIL"].ToString(), od["NAME"].ToString(), od["PHONE"].ToString(), od["ADDRESS"].ToString(), od["TEL"].ToString(), od["BIRTHDAY"].ToString(), od["PASSWORD"].ToString(), od["SEX"].ToString());
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

                sbSql.AppendFormat(@" SELECT  A.* FROM  OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI ') A");
                sbSql.AppendFormat(@" WHERE A.MI001 NOT IN (SELECT [MI001] FROM [test].[dbo].[WSCMI]) ");
                sbSql.AppendFormat(@"  AND A.FORM='WEB'");
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
                    INSERTLOGLOGWSCMISYNC("ADDTOMSSQLQSCMI", "RUN");
                }
            }
            catch
            {
                INSERTLOGLOGWSCMISYNC("ADDTOMSSQLQSCMI", "FAIL");
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
                sbSql.AppendFormat(" INSERT INTO [test].[dbo].[LOG_WSCMI]");
                sbSql.AppendFormat(" ([TRS_CODE],[TRS_DATE],[TRS_TIME],[store_ip],[sync_date],[sync_time],[sync_mark],[sync_count],[MI001])");
                sbSql.AppendFormat(" SELECT '2' AS [TRS_CODE],convert(varchar, getdate(), 112) AS [TRS_DATE],convert(varchar, getdate(), 108) AS [TRS_TIME]");
                sbSql.AppendFormat(" ,PI010 AS [store_ip]");
                sbSql.AppendFormat(" ,NULL AS [sync_date],NULL AS [sync_time],'N' AS [sync_mark],'0' AS [sync_count],TEMP.MI001 AS [MI001]");
                sbSql.AppendFormat(" FROM (SELECT MI001 FROM  OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI') A");
                sbSql.AppendFormat(" WHERE A.MI001 NOT IN (SELECT [MI001] FROM [test].[dbo].[WSCMI])) AS TEMP, [test].[dbo].[POSPI]");
                sbSql.AppendFormat(" ");
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

        public void UPDATETOMSSQLWSCMI()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();
                sbSql.AppendFormat(" UPDATE [test].[dbo].[LOG_WSCMI]");
                sbSql.AppendFormat(" SET [sync_mark]='N'");
                sbSql.AppendFormat(" WHERE [LOG_WSCMI].MI001 IN (SELECT A.MI001");
                sbSql.AppendFormat(" FROM  OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI') A");
                sbSql.AppendFormat(" INNER JOIN [test].[dbo].[WSCMI] B ON A.MI001=B.MI001");
                sbSql.AppendFormat(" WHERE A.FORM='WEB' AND (A.EMAIL<>B.MI031 OR A.NAME<>B.MI002 OR A.PHONE<>B.MI029 OR A.ADDRESS<>B.MI003 OR A.TEL<>B.MI004 OR convert(varchar, A.BIRTHDAY, 112)<>B.MI005 OR A.SEX <>B.MI010))");
                sbSql.AppendFormat(" ");
                sbSql.AppendFormat(" UPDATE [test].[dbo].[WSCMI]  ");
                sbSql.AppendFormat(" SET MI031=A.EMAIL,MI002=A.NAME,MI029=A.PHONE,MI003=A.ADDRESS,MI004=A.TEL,MI005=convert(varchar, A.BIRTHDAY,112),MI010= A.SEX");
                sbSql.AppendFormat(" FROM  OPENQUERY(MYSQL, 'SELECT MI001,EMAIL,NAME,PHONE,ADDRESS,TEL,BIRTHDAY,PASSWORD,SEX,FORM,STATUS FROM NEWDB.WSCMI') A");
                sbSql.AppendFormat(" INNER JOIN [test].[dbo].[WSCMI] B ON A.MI001=B.MI001");
                sbSql.AppendFormat(" WHERE A.FORM='WEB' AND (A.EMAIL<>B.MI031 OR A.NAME<>B.MI002 OR A.PHONE<>B.MI029 OR A.ADDRESS<>B.MI003 OR A.TEL<>B.MI004 OR convert(varchar, A.BIRTHDAY, 112)<>B.MI005 OR A.SEX <>B.MI010)");
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
                    INSERTLOGLOGWSCMISYNC("UPDATETOMSSQLWSCMI", "RUN");
                }

            }
            catch
            {
                INSERTLOGLOGWSCMISYNC("UPDATETOMSSQLWSCMI", "FAIL");
            }

            finally
            {
                sqlConn.Close();
            }
        }

        public void INSERTTOMYSQLWSCMIBOUNS()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.Clear();
                sbSql.AppendFormat(@" SELECT [ID],[MI001],[MI037OLD],[MI037NEW],CONVERT(varchar(100),[DATE],111) AS [DATE],[FORM],[STATUS]");
                sbSql.AppendFormat(@" FROM [TKWEBPOSSYNC].[dbo].[WSCMIBOUNS] A");
                sbSql.AppendFormat(@" WHERE[STATUS] = 'N'");
                sbSql.AppendFormat(@" AND NOT EXISTS (SELECT ID,FORM FROM  OPENQUERY(MYSQL, 'SELECT ID,FORM FROM NEWDB.WSCMIBOUNS') B WHERE A.ID=B.ID AND A.FORM=B.FORM)");
                sbSql.AppendFormat(@" ");
                sbSql.AppendFormat(@" ");

                adapter = new SqlDataAdapter(sbSql.ToString(), sqlConn);
                sqlCmdBuilder = new SqlCommandBuilder(adapter);

                sqlConn.Open();
                dsMYSQLWSCMIBOUNS.Clear();
                //dataGridView1.Columns.Clear();


                adapter.Fill(dsMYSQLWSCMIBOUNS, "MYSQLWSCMIBOUNS");
                sqlConn.Close();

                if (dsMYSQLWSCMIBOUNS.Tables["MYSQLWSCMIBOUNS"].Rows.Count == 0)
                {

                }
                else if (dsMYSQLWSCMIBOUNS.Tables["MYSQLWSCMIBOUNS"].Rows.Count >= 1)
                {
                    ADDTOMYSQLWSCMIBOUNS();
                    UPDATEMSSQLWSCMIBOUNS();
                    INSERTLOGWSCMIBOUNS("ADDTOMYSQLWSCMIBOUNS","RUN");
                }
            }
            catch
            {
                INSERTLOGWSCMIBOUNS("ADDTOMYSQLWSCMIBOUNS", "FAIL");
            }

            finally
            {
                sqlConn.Close();
            }
        }

        public void ADDTOMYSQLWSCMIBOUNS()
        {
            string connString = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;

            MySqlConnection conn = new MySqlConnection(connString);
            conn.Open();

            MySqlCommand AddNewCmd;
            StringBuilder AddNew = new StringBuilder();

            foreach (DataRow od in dsMYSQLWSCMIBOUNS.Tables["MYSQLWSCMIBOUNS"].Rows)
            {
                AddNew.AppendFormat(@" ");
                AddNew.AppendFormat(@" INSERT INTO NEWDB.WSCMIBOUNS(ID,MI001,MI037OLD,MI037NEW,DATE,FORM,STATUS) VALUES('{0}','{1}',{2},{3},'{4}','{5}','{6}'); ", od["ID"].ToString(), od["MI001"].ToString(), od["MI037OLD"].ToString(), od["MI037NEW"].ToString(), od["DATE"].ToString(), od["FORM"].ToString(), od["STATUS"].ToString());
                AddNew.AppendFormat(@" ");

            }

            AddNewCmd = new MySqlCommand(AddNew.ToString(), conn);
            AddNewCmd.Connection = conn;
            //執行新增
            AddNewCmd.ExecuteNonQuery();
        }

        public void UPDATEMSSQLWSCMIBOUNS()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();

                foreach (DataRow od in dsMYSQLWSCMIBOUNS.Tables["MYSQLWSCMIBOUNS"].Rows)
                {                   
                    sbSql.AppendFormat(" UPDATE [TKWEBPOSSYNC].[dbo].[WSCMIBOUNS]");
                    sbSql.AppendFormat(" SET [STATUS]='Y'");
                    sbSql.AppendFormat(" WHERE [ID]='{0}'", od["ID"].ToString());
                    sbSql.AppendFormat(" ");
                }


               
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
                    UPDATEMYSQLWSCMIBOUNS();
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
        public void INSERTTOMSSQLWSCMIBOUNS()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.Clear();
              
                sbSql.AppendFormat(@" SELECT A.MI001,SUM(A.MI037NEW-A.MI037OLD) AS BOUNS");
                sbSql.AppendFormat(@" FROM  OPENQUERY(MYSQL, 'SELECT  MI001,MI037OLD,MI037NEW FROM NEWDB.WSCMIBOUNS WHERE FORM=''WEB'' AND STATUS=''N''') A");
                sbSql.AppendFormat(@" INNER JOIN [test].dbo.WSCMI B ON A.MI001=B.MI001");
                sbSql.AppendFormat(@" GROUP BY A.MI001");
                sbSql.AppendFormat(@" ");

                adapter = new SqlDataAdapter(sbSql.ToString(), sqlConn);
                sqlCmdBuilder = new SqlCommandBuilder(adapter);

                sqlConn.Open();
                dsMSSQLWSCMIBOUNS.Clear();
                //dataGridView1.Columns.Clear();


                adapter.Fill(dsMSSQLWSCMIBOUNS, "MSSQLWSCMIBOUNS");
                sqlConn.Close();

                if (dsMSSQLWSCMIBOUNS.Tables["MSSQLWSCMIBOUNS"].Rows.Count == 0)
                {

                }
                else if (dsMSSQLWSCMIBOUNS.Tables["MSSQLWSCMIBOUNS"].Rows.Count >= 1)
                {
                    ADDTOMSSQLWSCMI();                    
                    INSERTLOGWSCMIBOUNS("ADDTOMSSQLWSCMI", "RUN");
                }
            }
            catch
            {
                INSERTLOGWSCMIBOUNS("ADDTOMSSQLWSCMI", "FAIL");
            }

            finally
            {
                sqlConn.Close();
            }
        }

        public void ADDTOMSSQLWSCMI()
        {
            string DAYNO = DateTime.Now.ToString("yyyyMMdd");
            string NP001=GETMAXTWSCNP(DAYNO);
            string TU006 = "0"+NP001.Substring(8, 3);

            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();

                sbSql.AppendFormat(" INSERT INTO [test].[dbo].[WSCNP]");
                sbSql.AppendFormat(" ([COMPANY],[CREATOR],[USR_GROUP],[CREATE_DATE],[MODIFIER],[MODI_DATE],[FLAG],[CREATE_TIME],[MODI_TIME],[TRANS_TYPE]");
                sbSql.AppendFormat(" ,[TRANS_NAME],[sync_date],[sync_time],[sync_mark],[sync_count],[DataUser],[DataGroup],[NP001],[NP002],[NP003]");
                sbSql.AppendFormat(" ,[NP004],[NP005],[NP006],[NP007],[NP008],[NP009],[NP010],[NP011],[NP012],[UDF01]");
                sbSql.AppendFormat(" ,[UDF02],[UDF03],[UDF04],[UDF05],[UDF06],[UDF07],[UDF08],[UDF09],[UDF10])");
                sbSql.AppendFormat(" VALUES('test','DS','DS','{0}','DS','{1}','2',convert(varchar, getdate(), 108),convert(varchar, getdate(), 108),'P001',", DAYNO, DAYNO);
                sbSql.AppendFormat(" 'POSI14',NULL,NULL,NULL,0,NULL,'DS','{0}','{1}','DS',", NP001, DAYNO);
                sbSql.AppendFormat(" 'Y','{0}',NULL,NULL,NULL,NULL,NULL,0,0,NULL,", DAYNO);
                sbSql.AppendFormat(" NULL,NULL,NULL,NULL,0,0,0,0,0)");
                sbSql.AppendFormat(" ");

                foreach (DataRow od in dsMSSQLWSCMIBOUNS.Tables["MSSQLWSCMIBOUNS"].Rows)
                {
                    sbSql.AppendFormat(" INSERT INTO [test].[dbo].[WSCNQ]");
                    sbSql.AppendFormat(" ([COMPANY],[CREATOR],[USR_GROUP],[CREATE_DATE],[MODIFIER],[MODI_DATE],[FLAG],[CREATE_TIME],[MODI_TIME],[TRANS_TYPE]");
                    sbSql.AppendFormat(" ,[TRANS_NAME],[sync_date],[sync_time],[sync_mark],[sync_count],[DataUser],[DataGroup],[NQ001],[NQ002],[NQ003]");
                    sbSql.AppendFormat(" ,[NQ004],[NQ005],[NQ006],[NQ007],[NQ008],[NQ009],[NQ010],[NQ011],[NQ012],[NQ013]");
                    sbSql.AppendFormat(" ,[NQ014],[NQ015],[NQ016],[UDF01],[UDF02],[UDF03],[UDF04],[UDF05],[UDF06],[UDF07]");
                    sbSql.AppendFormat(" ,[UDF08],[UDF09],[UDF10])");
                    sbSql.AppendFormat(" VALUES('test','DS','DS','{0}','DS','{1}','2',convert(varchar, getdate(), 108),convert(varchar, getdate(), 108),'P001', ", DAYNO, DAYNO);
                    sbSql.AppendFormat(" 'POSI14',NULL,NULL,NULL,0,NULL,'DS','{0}','{1}',NULL,", NP001, od["MI001"].ToString());
                    sbSql.AppendFormat(" NULL,{0},NULL,'Y',NULL,NULL,NULL,NULL,NULL,0,", od["BOUNS"].ToString());
                    sbSql.AppendFormat(" 0,0,NULL,NULL,NULL,NULL,NULL,NULL,0,0,");
                    sbSql.AppendFormat(" 0,0,0)");
                    sbSql.AppendFormat(" ");
                }

                foreach (DataRow od in dsMSSQLWSCMIBOUNS.Tables["MSSQLWSCMIBOUNS"].Rows)
                {
                    sbSql.AppendFormat(" INSERT INTO [test].[dbo].[POSTU]");
                    sbSql.AppendFormat(" ([COMPANY],[CREATOR],[USR_GROUP],[CREATE_DATE],[MODIFIER],[MODI_DATE],[FLAG],[CREATE_TIME],[MODI_TIME],[TRANS_TYPE]");
                    sbSql.AppendFormat(" ,[TRANS_NAME],[sync_date],[sync_time],[sync_mark],[sync_count],[DataUser],[DataGroup],[TU001],[TU002],[TU003]");
                    sbSql.AppendFormat(" ,[TU004],[TU005],[TU006],[TU007],[TU008],[TU009],[TU010],[TU011],[TU012],[TU013]");
                    sbSql.AppendFormat(" ,[TU014],[TU015],[TU016],[TU017],[TU018],[TU019],[TU020],[TU021],[TU022],[TU023]");
                    sbSql.AppendFormat(" ,[TU024],[TU025],[TU026],[TU027],[TU028],[TU029],[UDF01],[UDF02],[UDF03],[UDF04]");
                    sbSql.AppendFormat(" ,[UDF05],[UDF06],[UDF07],[UDF08],[UDF09],[UDF10])");
                    sbSql.AppendFormat(" VALUES('test','DS','DS','{0}','DS','{1}','2',convert(varchar, getdate(), 108),convert(varchar, getdate(), 108),'P004',", DAYNO, DAYNO);
                    sbSql.AppendFormat(" 'POSI14',NULL,NULL,NULL,0,NULL,'DS','{0}','TK','000',", DAYNO);
                    sbSql.AppendFormat(" '{0}',convert(varchar, getdate(), 108),'{1}','{2}','A','{3}','POSI14','N','2','N',", DAYNO, TU006, od["MI001"].ToString(), od["BOUNS"].ToString());
                    sbSql.AppendFormat(" NULL,NULL,NULL,0,0,'{0}',0,0,0,0,", NP001);
                    sbSql.AppendFormat(" NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,");
                    sbSql.AppendFormat(" NULL,0,0,0,0,0)");
                    sbSql.AppendFormat(" ");
                }

                sbSql.AppendFormat(" UPDATE [test].[dbo].[WSCMI]");
                sbSql.AppendFormat(" SET [WSCMI].MI037=[WSCMI].MI037+TEMP.BOUNS");
                sbSql.AppendFormat(" FROM [test].[dbo].[WSCMI]");
                sbSql.AppendFormat(" INNER JOIN (SELECT A.MI001,SUM(A.MI037NEW-A.MI037OLD) AS BOUNS");
                sbSql.AppendFormat(" FROM  OPENQUERY(MYSQL, 'SELECT  MI001,MI037OLD,MI037NEW FROM NEWDB.WSCMIBOUNS WHERE FORM=''WEB'' AND STATUS=''N''') A");
                sbSql.AppendFormat(" INNER JOIN [test].dbo.WSCMI B ON A.MI001=B.MI001");
                sbSql.AppendFormat(" GROUP BY A.MI001) AS TEMP ON [WSCMI].MI001=TEMP.MI001");
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
                    DELWSCMIBOUNS(); //因為TRIIGER會在WEB修改點數到POS時，又產生資料從POS到WEB，故刪除 
                    UPDATEMYSQLWSCMIBOUNS();
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

        public void DELWSCMIBOUNS()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dbconn"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sqlConn.Close();
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();

                foreach (DataRow od in dsMSSQLWSCMIBOUNS.Tables["MSSQLWSCMIBOUNS"].Rows)
                {
                    sbSql.AppendFormat(" DELETE [TKWEBPOSSYNC].[dbo].[WSCMIBOUNS]");
                    sbSql.AppendFormat(" WHERE [STATUS]='N' AND [MI001]='{0}' AND ([MI037NEW]-[MI037OLD])='{1}';", od["MI001"].ToString(), od["BOUNS"].ToString());
                    sbSql.AppendFormat(" ");
                }



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
                    UPDATEMYSQLWSCMIBOUNS();
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
        public void UPDATEMYSQLWSCMIBOUNS()
        {
            string connString = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;

            MySqlConnection conn = new MySqlConnection(connString);
            conn.Open();

            MySqlCommand AddNewCmd;
            StringBuilder AddNew = new StringBuilder();

            foreach (DataRow od in dsMSSQLWSCMIBOUNS.Tables["MSSQLWSCMIBOUNS"].Rows)
            {
                AddNew.AppendFormat(@" UPDATE NEWDB.WSCMIBOUNS SET STATUS='Y' WHERE FORM='WEB' AND MI001='{0}' ;" ,od["MI001"].ToString());
                AddNew.AppendFormat(@" ");
                AddNew.AppendFormat(@" ");

            }

            AddNewCmd = new MySqlCommand(AddNew.ToString(), conn);
            AddNewCmd.Connection = conn;
            //執行新增
            AddNewCmd.ExecuteNonQuery();
        }
        public string GETMAXTWSCNP(string DAYNO)
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dberp"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);
                DataSet ds = new DataSet();
                SqlDataAdapter adapter = new SqlDataAdapter();
                SqlCommandBuilder sqlCmdBuilder = new SqlCommandBuilder();
                StringBuilder sbSql = new StringBuilder();

                string NP001;

                sbSql.Clear();

                ds.Clear();

                sbSql.AppendFormat(@"   SELECT ISNULL(MAX(NP001),'00000000000') AS NP001");
                sbSql.AppendFormat(@"  FROM [test].[dbo].[WSCNP] ");
                sbSql.AppendFormat(@"  WHERE  NP001 LIKE '{0}%' ", DAYNO);
                sbSql.AppendFormat(@"  ");
                sbSql.AppendFormat(@"  ");

                adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);

                sqlCmdBuilder = new SqlCommandBuilder(adapter);
                sqlConn.Open();
                ds.Clear();
                adapter.Fill(ds, "ds");
                sqlConn.Close();


                if (ds.Tables["ds"].Rows.Count == 0)
                {
                    return null;
                }
                else
                {
                    if (ds.Tables["ds"].Rows.Count >= 1)
                    {
                        NP001 = SETTNP001(DAYNO, ds.Tables["ds"].Rows[0]["NP001"].ToString());
                        return NP001;

                    }
                    return null;
                }

            }
            catch
            {
                return null;
            }
            finally
            {
                sqlConn.Close();
            }

            
        }

        public string SETTNP001(string DAYNO, string NP001)
        {
            if (NP001.Equals("00000000000"))
            {
                return DAYNO + "001";
            }

            else
            {
                int serno = Convert.ToInt16(NP001.Substring(8, 3));
                serno = serno + 1;
                string temp = serno.ToString();
                temp = temp.PadLeft(3, '0');
                return DAYNO + temp.ToString();
            }
        }
        public void INSERTLOGLOGWSCMISYNC(string PROG, string message)
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
                sbSql.AppendFormat(" ([PROG],[EXECTIME],[STATUS])");
                sbSql.AppendFormat("  VALUES ('{0}','{1}','{2}')", PROG, DateTime.Now.ToString("yyyy/MM/dd hh:mm:dd"), message);
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
        public void INSERTLOGWSCMIBOUNS(string PROG,string message)
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
                sbSql.AppendFormat(" ([PROG],[EXECTIME],[STATUS])");
                sbSql.AppendFormat("  VALUES ('{0}','{1}','{2}')", PROG, DateTime.Now.ToString("yyyy/MM/dd hh:mm:dd"), message);
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


        #region
        private void button1_Click(object sender, EventArgs e)
        {
            if (button1.Text.Equals("啟動"))
            {
                button1.Text = "停止";
                label1.Text = "RUNNING";
                SYNC = "Y";
            }
            else
            {
                button1.Text = "啟動";
                label1.Text = "STOP ";
                SYNC = "N";
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            //執行順序不可改，會造成點數重覆寫到WEB、POS
            INSERTTOMSSQLWSCMI();
            UPDATETOMSSQLWSCMI();
           
            INSERTTOMSSQLWSCMIBOUNS();
            INSERTTOMYSQLWSCMIBOUNS();

            INSERTTOMYSQLWSCMISYNC();
            UPDATEMYSQLWSCMISYNC();
        }

        #endregion

        
    }
}
