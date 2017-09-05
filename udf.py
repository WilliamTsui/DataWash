#!-*-coding=utf-8-*-

import re


"""def float_format(data):
    if data in [' ',None,'null','']:
        return data
    if re.match(r'\D',data):
        return data
    try:
        datainfo='%.2f'%data
        return datainfo
    except:
        return data"""
    
def float_format(data):
    if data in ['',None,' ','Null',0,'0','etldr_none','etldr_null']:
        return data
    try:
        if '-' in str(data):
            return float(data)        
        elif '.' in str(data):
            datainfo=str(data).split('.')
            if len(datainfo[1])==2:
                return float(data)
            elif len(datainfo[1])<2:
                return float(str(data)+'0'*(2-len(datainfo[1])))
            elif len(datainfo[1])>2:
                return float(str(datainfo[0])+'.'+str(datainfo[1][:2]))
        else:
            return float(str(data)+'.'+'0'*2) 
    except:
        return data


def space_format(data):
    try:
        if data in ['',None,' ','Null',0,'0','etldr_none','etldr_null']:
            return data      
        else:
            resData=data.strip()
            return resData
    except:
        return data
    
udf={
    'float_format':float_format,
    'space_format':space_format
    }

if __name__ == '__main__':
    a=2.0152414546333
    print float_format(a)

