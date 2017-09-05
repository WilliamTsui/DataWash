#£¡-*-coding=utf-8-*-

from Connect import ConnDB
from udf import udf as udfInfo
import ujson as json
import datetime
import os,sys,re
reload(sys)
sys.setdefaultencoding('UTF8')
import copy
import pyspark
#------------------------------------------------------------------------------ 
#===============================================================================
# Import PySpark package
#===============================================================================
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark import SparkSQL
    from pyspark import SQLContext
except:
    pass
now = datetime.datetime.now()
print "-"*5+"%s,Import the package is Successful!"%str(now)+"-"*5 
#------------------------------------------------------------------------------
#===============================================================================
# Spark Config
#===============================================================================
print "-"*5+"%s,Start Create Spark Context!"%str(now)+"-"*5
def ConnSpark():
    conf=SparkConf()
    conf.setAppName('DataWash')
    conf.setMaster('local')
    return SparkContext(conf=conf)
#------------------------------------------------------------------------------ 
sc=ConnSpark()
SqlCtx=pyspark.sql.SQLContext(sc)
print "-"*5+"%s,Create Spark Context is finish!"%str(now)+"-"*5 
#------------------------------------------------------------------------------ 
#===============================================================================
# Register udf Function
#===============================================================================
print "-"*5+"%s,Register the function!"%str(now)+"-"*5
for udfFunction in udfInfo:
    SqlCtx.registerFunction(udfFunction,udfInfo[udfFunction])
    print udfInfo[udfFunction]
print "-"*5+"%s,Register the function finish!"%str(now)+"-"*5
#------------------------------------------------------------------------------ 
hdfsPath='/user/hive/warehouse'
#------------------------------------------------------------------------------
#===============================================================================
# Return the information which the column need to Wash
#===============================================================================
print "-"*5+"%s,Get the Column which need to Wash!"%str(now)+"-"*5+'\n'
WashInfoSQL='select distinct table_name,db_name from datawash_config where type=0'
result=ConnDB(WashInfoSQL).ConnMysql()
print "-"*5+"%s,Get the Column Finish!"%str(now)+"-"*5+'\n'
#------------------------------------------------------------------------------
#===============================================================================
# Start Wash
#===============================================================================
print "-"*5+"%s,Start Wash action!"%str(now)+"-"*5+'\n'
for i in result:
    print i
    WashInfoList=[]
    WashCoLList=[]
    AllColumns=[]
    ColTypeString=[]
    OtherColinfo=[]
    TabName=i[0]
    DbName=i[1]
    WashCOLInfo="select column_name,wash_type from datawash_config where table_name='%s' and db_name='%s' and type=0"%(TabName,DbName)
    DbTable=SqlCtx.read.json('%s/%s.db/%s'%(hdfsPath,DbName,TabName))
#------------------------------------------------------------------------------ 
    #===========================================================================
    # Print the Schema
    #===========================================================================
    DbTable.printSchema()
    TabColumn=copy.deepcopy(DbTable.columns)
    for Colinfomation in DbTable.dtypes:
        ColName=Colinfomation[0]
        ColType=Colinfomation[1]
        AllColumns.append(ColName)
        if ColType=='string':
            ColTypeString.append('space_format(%s) as %s'%(ColName,ColName))
        else:
            OtherColinfo.append(ColName)
    
    if AllColumns==[]:
        pass
    else:
        DbTable.registerTempTable('%s_temp'%TabName)
        if ColTypeString==[]:
            StringSql='select %s from %s_temp'%(','.join(AllColumns),TabName)
        elif OtherColinfo==[]:
            StringSql='select %s from %s_temp'%(','.join(ColTypeString),TabName)
        else:
            StringSql='select %s,%s from %s_temp'%(','.join(ColTypeString),','.join(OtherColinfo),TabName)
    #===========================================================================
    # Print the sql which Wash the column with String_format
    #===========================================================================
    print  'String-SQL:'+StringSql+'\n'  
    DbTable=SqlCtx.sql(StringSql)
    DbTable.registerTempTable('%s_bak'%TabName)
                
    for i in ConnDB(WashCOLInfo).ConnMysql():
        ColNames=i[0]
        WashType=i[1]
        WashCoLList.append(ColNames)
        WashInfoList.append('%s(%s) as %s'%(WashType,ColNames,ColNames))
    dataWashList=list(set(AllColumns)^set(WashCoLList))
    
    if AllColumns==[] and WashCoLList==[]:
        pass
    else:
        if dataWashList==[]:
            WashSql='select %s from %s_bak'%(','.join(WashInfoList),TabName)
        else:
            WashSql='select %s,%s from %s_bak'%(','.join(WashInfoList),','.join(dataWashList),TabName)
    #===========================================================================
    # Print the sql which the column need to specify wash
    #===========================================================================
    print 'Specify-SQL:'+WashSql+'\n'
    DbTable=SqlCtx.sql(WashSql)
    DbTable.registerTempTable('%s_ready'%TabName)
    DbTable.persist()
    print "-"*5+"%s,Write Data into the table !"%str(now)+"-"*5+'\n'
    DbTable.write.saveAsTable('shares_info_json',format='json',mode='overwrite',path='hdfs://dch01:9000/user/hive/warehouse/test.db/shares_info_json')
    print "-"*5+"%s,Write data is Finish!"%str(now)+"-"*5+'\n'
    print "-"*5+'Wash table %s is Successful!'%TabName+"-"*5+'\n'
#------------------------------------------------------------------------------ 
    ModifySQLMysql='update datawash.datawash_config set type=1'
    ConnDB(ModifySQLMysql).ConnMysql()
    ModifySQLtime="update datawash.datawash_config set lastmodify='%s'"%now
    ConnDB(ModifySQLtime).ConnMysql()
    print "-"*5+"Modify Configure information is successful!"+"-"*5+'\n'
sc.stop()