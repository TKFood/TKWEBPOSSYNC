using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;
using System.Configuration;
using System.Reflection;
using System.Threading;
using System.Globalization;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace TKWEBPOSSYNC
{
    public partial class Form1 : Form
    {
        private ComponentResourceManager _ResourceManager = new ComponentResourceManager();
        SqlConnection sqlConn = new SqlConnection();
        SqlCommand sqlComm = new SqlCommand();
        string connectionString;
        StringBuilder sbSql = new StringBuilder();
        SqlDataAdapter adapter1 = new SqlDataAdapter();
        SqlCommandBuilder sqlCmdBuilder1 = new SqlCommandBuilder();
        DataSet ds1 = new DataSet();

        DataTable table = new DataTable();
        DataTable MStable = new DataTable();
        DataTable MYtable = new DataTable();


        public Form1()
        {
            InitializeComponent();
        }


        #region FUNCTION
        public void SEARCHMYSQL()
        {
            //string dbHost = "";//資料庫位址
            //string dbUser = "";//資料庫使用者帳號
            //string dbPass = "";//資料庫使用者密碼
            //string dbName = "";//資料庫名稱
            //string connStr = "server=" + dbHost + ";uid=" + dbUser + ";pwd=" + dbPass + ";database=" + dbName;
            //string connStr = "server=192.168.1.170;uid=tk;pwd=Tk2vjx;database=NEWDB";

            string connStr = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;
           

            MySqlConnection conn = new MySqlConnection(connStr);
            MySqlCommand command = conn.CreateCommand();
            conn.Open();
    
            MySqlDataAdapter MyDA = new MySqlDataAdapter();
            string sqlSelectAll = "SELECT NAME,EMAIL FROM NEWDB.NEWTB";
            MyDA.SelectCommand = new MySqlCommand(sqlSelectAll, conn);

            table.Clear();
            MyDA.Fill(table);

            BindingSource bSource = new BindingSource();
            bSource.DataSource = table;

            dataGridView1.DataSource = bSource;
            conn.Close();

        }

        public void SEARCHTKWEBPOSSYNC()
        {
            try
            {
                connectionString = ConfigurationManager.ConnectionStrings["dberp"].ConnectionString;
                sqlConn = new SqlConnection(connectionString);

                sbSql.Clear();   
            
                sbSql.AppendFormat(@" SELECT [NAME],[EMAIL] FROM [TKWEBPOSSYNC].[dbo].[NEWTB] ");

                adapter1 = new SqlDataAdapter(@"" + sbSql, sqlConn);

                sqlCmdBuilder1 = new SqlCommandBuilder(adapter1);
                sqlConn.Open();
                ds1.Clear();
                adapter1.Fill(ds1, "TEMPds1");
                sqlConn.Close();


                if (ds1.Tables["TEMPds1"].Rows.Count == 0)
                {

                }
                else
                {
                    if (ds1.Tables["TEMPds1"].Rows.Count >= 1)
                    {
                        //dataGridView1.Rows.Clear();
                        dataGridView1.DataSource = ds1.Tables["TEMPds1"];
                        dataGridView1.AutoResizeColumns();
                        //dataGridView1.CurrentCell = dataGridView1[0, rownum];

                    }
                }

            }
            catch
            {

            }
            finally
            {

            }
        }

        public void SEARCHEXCEPT()
        {
            if(table.Rows.Count>=1 && ds1.Tables["TEMPds1"].Rows.Count>=1)
            {
                MYtable = table;
                MStable = ds1.Tables["TEMPds1"];

                var DIFF = MYtable.AsEnumerable().Except(MStable.AsEnumerable(), DataRowComparer.Default);


                // Create a table from the query.
                DataTable DIFFTABLE = DIFF.CopyToDataTable<DataRow>();

                if (DIFFTABLE.Rows.Count >= 1)
                {
                    dataGridView1.DataSource = DIFFTABLE;
                    dataGridView1.AutoResizeColumns();
                }

            }
        }


        #endregion

        #region BUTTON

        private void button1_Click(object sender, EventArgs e)
        {
            SEARCHMYSQL();
        }

        #endregion

        private void button2_Click(object sender, EventArgs e)
        {
            SEARCHTKWEBPOSSYNC();
        }

        private void button3_Click(object sender, EventArgs e)
        {
            SEARCHEXCEPT();

        }
    }
}
