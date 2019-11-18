## Section to import required libraries
import pyspark.sql.functions as func
from pyspark.sql.functions import udf,col,max,min,hour,minute,lit,sum,stddev
from pyspark.sql.types import FloatType
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext 
from pyspark.sql import SparkSession
import json
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import  from_unixtime,unix_timestamp
from pyspark.sql.functions import to_timestamp,col,lit 
from datetime import *
from datetime import datetime,timedelta
from pyspark.sql.functions import coalesce, lit, col, lead, lag
from operator import add
from pyspark.sql.window import Window


spark.sparkContext.stop()

#Setting up SparkSession
from pyspark.sql import SparkSession
conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '8g'), 
                                        ('spark.executor.cores', '2'), 
                                        ('spark.cores.max', '4'), 
                                        ('spark.driver.memory','8g')])
spark = SparkSession.builder.config(conf=conf) 

spark.conf.set("spark.cassandra.connection.host", "153.88.12.14")
spark.conf.set("spark.cassandra.connection.port","31156")


analysisCols=['L_Thrp_bits_DL','L_Thrp_bits_UL','L_ChMeas_PRB_DL_Used_Avg','L_ChMeas_PRB_DL_Avail','L_Traffic_User_Avg','L_UL_Interference_Avg']
allCols=['objectname','ossdate','osshour','ossresulttime','L_Thrp_bits_DL','L_Thrp_bits_UL','L_ChMeas_PRB_DL_Used_Avg','L_ChMeas_PRB_DL_Avail','L_Traffic_User_Avg','L_UL_Interference_Avg','l_traffic_ul_pktloss_loss_qci_5','l_e_rab_attest_qci_2','l_thrp_time_dl_qci_2','l_traffic_dl_pktdelay_time','l_e_rab_abnormrel_radio_ulsyncfail','l_e_rab_abnormrel_hofailure_voip','l_e_rab_rel_mme_qci_3','l_traffic_dl_pktuuloss_loss_qci_2','l_e_rab_sessiontime_highprecision_qci2','l_e_rab_abnormrel_hoout','l_rrc_connreq_att_highpri','l_e_rab_succest_hoin_qci_3','l_e_rab_abnormrel_enbtot_qci_2','l_e_rab_abnormrel_tnl_other_voip','l_e_rab_attest_hoin_qci_3','l_thrp_time_dl_qci_8','l_traffic_dl_pktdelay_num_qci_1','l_traffic_ul_pktloss_loss_qci_7','l_e_rab_rel_enodeb_qci_2','l_thrp_time_dl_rmvlasttti_wbb','l_thrp_time_dl','l_thrp_time_dl_rmvlasttti_qci_8','l_e_rab_attest_hoin','l_thrp_bits_ue_ul_specifictti','l_thrp_bits_dl_max','l_thrp_bitrate_dl_srb','l_thrp_transfertime_dl_rmvlasttti','l_e_rab_initsuccest_qci_8','l_traffic_dl_pktdelay_time_qci_8','l_rrc_connreq_att_mt','l_traffic_dl_emptybuf_pdcplat_time','l_thrp_bits_dl_srb','l_e_rab_attest_hoin_qci_8','l_traffic_dl_pktuuloss_tot_qci_8','l_e_rab_succest','l_thrp_time_cell_ul_highprecision','l_e_rab_initsuccest','l_e_rab_abnormrel_other_voip','l_traffic_dl_pktdelay_num']
impCols=['objectname','ossdate','osshour','ossresulttime','l_traffic_ul_pktloss_loss_qci_5','l_e_rab_attest_qci_2','l_thrp_time_dl_qci_2','l_traffic_dl_pktdelay_time','l_e_rab_abnormrel_radio_ulsyncfail','l_e_rab_abnormrel_hofailure_voip','l_e_rab_rel_mme_qci_3','l_traffic_dl_pktuuloss_loss_qci_2','l_e_rab_sessiontime_highprecision_qci2','l_e_rab_abnormrel_hoout','l_rrc_connreq_att_highpri','l_e_rab_succest_hoin_qci_3','l_e_rab_abnormrel_enbtot_qci_2','l_e_rab_abnormrel_tnl_other_voip','l_e_rab_attest_hoin_qci_3','l_thrp_time_dl_qci_8','l_traffic_dl_pktdelay_num_qci_1','l_traffic_ul_pktloss_loss_qci_7','l_e_rab_rel_enodeb_qci_2','l_thrp_time_dl_rmvlasttti_wbb','l_thrp_time_dl','l_thrp_time_dl_rmvlasttti_qci_8','l_e_rab_attest_hoin','l_thrp_bits_ue_ul_specifictti','l_thrp_bits_dl_max','l_thrp_bitrate_dl_srb','l_thrp_transfertime_dl_rmvlasttti','l_e_rab_initsuccest_qci_8','l_traffic_dl_pktdelay_time_qci_8','l_rrc_connreq_att_mt','l_traffic_dl_emptybuf_pdcplat_time','l_thrp_bits_dl_srb','l_e_rab_attest_hoin_qci_8','l_traffic_dl_pktuuloss_tot_qci_8','l_e_rab_succest','l_thrp_time_cell_ul_highprecision','l_e_rab_initsuccest','l_e_rab_abnormrel_other_voip','l_traffic_dl_pktdelay_num']

currentTime=datetime.now()
lags=currentTime - timedelta(minutes=120)

#Change this based on the time zone difference
timdedifference = 150

Full_DF = spark.read.format("org.apache.spark.sql.cassandra")\
    .options(table='huawei_4g', keyspace='common')\
    .load()\
    .select(*allCols)\
    .withColumn('ossresulttime',func.to_timestamp("ossresulttime", "yyyy-MM-dd HH:mm"))\
    .withColumn('ossdate',func.to_timestamp("ossdate", "yyyy-MM-dd"))\
    .withColumn('osshour',col('osshour').cast('float'))\
    .filter(col("ossresulttime") > datetime.now()+timedelta(minutes=timdedifference))

combined_DF=Full_DF.select(*impCols)
maxTime=combined_DF.select(func.max('ossresulttime')).first()
combined_DF=combined_DF.filter(combined_DF['ossresulttime'] >= maxTime[0] - timedelta(minutes=15))

combined_DF=combined_DF.na.fill({'l_traffic_ul_pktloss_loss_qci_5' :  9.32716621103459e-06,
'l_e_rab_attest_qci_2' :  5.48632216489652e-05,
'l_thrp_time_dl_qci_2' :  0.0151406993665932,
'l_traffic_dl_pktdelay_time' :  128644162.964624,
'l_e_rab_abnormrel_radio_ulsyncfail' :  5.39333365362709e-05,
'l_e_rab_abnormrel_hofailure_voip' :  1.23984681692576e-06,
'l_e_rab_rel_mme_qci_3' :  3.40957874654586e-06,
'l_traffic_dl_pktuuloss_loss_qci_2' :  1.92761435028048e-05,
'l_e_rab_sessiontime_highprecision_qci2' :  0.0069874666984894,
'l_e_rab_abnormrel_hoout' :  1.85977022538865e-06,
'l_rrc_connreq_att_highpri' :  0.0077239357077433,
'l_e_rab_succest_hoin_qci_3' :  2.47969363385153e-06,
'l_e_rab_abnormrel_enbtot_qci_2' :  3.09961704231442e-07,
'l_e_rab_abnormrel_tnl_other_voip' :  1.08486596481004e-05,
'l_e_rab_attest_hoin_qci_3' :  2.47969363385153e-06,
'l_thrp_time_dl_qci_8' :  1360896.70088323,
'l_traffic_dl_pktdelay_num_qci_1' :  33.0728022631436,
'l_traffic_ul_pktloss_loss_qci_7' :  0.00502765349328801,
'l_e_rab_rel_enodeb_qci_2' :  2.16973192962009e-06,
'l_thrp_time_dl_rmvlasttti_wbb' :  405345.760519434,
'l_thrp_time_dl' :  1361887.99113261,
'l_thrp_time_dl_rmvlasttti_qci_8' :  785230.227870516,
'l_e_rab_attest_hoin' :  691.167969487369,
'l_thrp_bits_ue_ul_specifictti' :  407876365.860731,
'l_thrp_bits_dl_max' :  31784443.1460927,
'l_thrp_bitrate_dl_srb' :  1097.18144352265,
'l_thrp_transfertime_dl_rmvlasttti' :  350298.851309648,
'l_e_rab_initsuccest_qci_8' :  879.121027956995,
'l_traffic_dl_pktdelay_time_qci_8' :  128574917.380728,
'l_rrc_connreq_att_mt' :  446.10230627006,
'l_traffic_dl_emptybuf_pdcplat_time' :  845036.160484465,
'l_thrp_bits_dl_srb' :  2464782.86136683,
'l_e_rab_attest_hoin_qci_8' :  688.955505927242,
'l_traffic_dl_pktuuloss_tot_qci_8' :  1280498.87629224,
'l_e_rab_succest' :  913.821447490162,
'l_thrp_time_cell_ul_highprecision' :  485239.455832781,
'l_e_rab_initsuccest' :  881.416139705939,
'l_e_rab_abnormrel_other_voip' :  3.09961704231442e-07,
'l_traffic_dl_pktdelay_num' :  1278076.18881822,
})

combined_DF=combined_DF.withColumn('siteid',col('objectname'))
combined_DF=combined_DF.withColumnRenamed('objectname','cellid')
combined_DF=combined_DF.withColumnRenamed('ossresulttime','datetime')

combined_DF.write.format("org.apache.spark.sql.cassandra")\
                .options(table='huaw4g_trpt_hrly', keyspace='normalized')\
                .mode('Append')\
                .save()

MA_df=Full_DF.select(*allCols)
avgCols=MA_df.columns[10:]


def moving_averages(dataframe, cols, min_lag, max_lag): 
    w =  Window.partitionBy(MA_df['objectname']).orderBy(MA_df['ossresulttime']).rowsBetween(-max_lag, -min_lag)
    out = dataframe.select('objectname','ossresulttime','ossdate','osshour','L_Thrp_bits_DL','L_Thrp_bits_UL','L_ChMeas_PRB_DL_Used_Avg','L_ChMeas_PRB_DL_Avail','L_Traffic_User_Avg','L_UL_Interference_Avg',
                    *[func.avg(i).over(w).alias(i+"_ma") for i in cols])
    return out


Final_dataset= moving_averages(MA_df, avgCols, 0,2)

def create_kpi(df):
    df=df.withColumn('siteid',col('objectname'))
    df=df.withColumnRenamed('objectname','cellid')
    df=df.withColumnRenamed('ossresulttime','datetime')


    df=df.withColumn('kpi_datavolume',((Final_dataset['l_thrp_bits_dl']+Final_dataset['l_thrp_bits_ul'])/8)/(1024*1024))
    df=df.withColumn('kpi_dlprbutilizationrate',(Final_dataset['l_chmeas_prb_dl_used_avg']/Final_dataset['l_chmeas_prb_dl_avail'])*100)
    df=df.withColumn('kpi_uplinkinterference',Final_dataset['l_ul_interference_avg'])

    df=df.drop(*analysisCols)

    df=df.withColumn("sleepycell_alarms",func.lit(0))
    df=df.withColumn("outage_alarms",func.lit(0))
    df=df.withColumn("qia_alarms",func.lit(0))

    return df

Final_dataset = create_kpi(Final_dataset)

Final_dataset.write.format("org.apache.spark.sql.cassandra")\
                   .options(table='huaw4g_kpid_trpt_comp', keyspace='normalized')\
                   .mode('Append').save() ## override

