# -*- coding: utf-8 -*-
"""
TAN Xiao

This is a temporary script file.
"""

import pandas as pd
import numpy as np

#--------Data Loading-----------#
data_alias="C:/Users/smile/Downloads/9102/Assignment 1/"
data_type=".tsv"
file_name_1="R_sorted"
file_name_2="S_sorted"
file_name_3="R"
s="\t"

R_sorted=pd.read_csv(data_alias+file_name_1+data_type,sep=s,header=None,names=["Index","Value"]).fillna("Not Applicable")

S_sorted=pd.read_csv(data_alias+file_name_2+data_type,sep=s,header=None,names=["Index","Value"]).fillna("Not Applicable")

#--------Assigment 1------------#

#Q1  Merge-join (25%)#

#R_sorted=R_sorted.query("Index=="zvv"")
#S_sorted=S_sorted.query("Index=="zvv"")

RjoinS=[]
lines=0

R_sorted_=R_sorted
S_sorted_=S_sorted

for idx in (R_sorted_["Index"]):

    try:
        R_=R_sorted_[["Index","Value"]][R_sorted_["Index"]==idx]
        S_=S_sorted_[["Index","Value"]][S_sorted_["Index"]==idx]
        #print(idx,R_)
        R_=R_.reset_index()
        S_=S_.reset_index()
        n_r=len(R_)
        n_s=len(S_)
        for i in range (n_r):
            for j in range (n_s):
                RjoinS_=[idx,R_["Value"][i],S_["Value"][j]]
                print(lines,RjoinS_)
                RjoinS.append(RjoinS_)
                j+=1
                lines+=1   
            i+=1
        R_sorted_=R_sorted_[R_sorted_.Index != idx]
        S_sorted_=S_sorted_[S_sorted_.Index != idx]
    except ValueError:
        pass


'''
for i in range(len(R_sorted)):
    R_sorted=R_sorted[["Index","Value"]].reset_index()
    try:
        for j in range(len(S_sorted)):
            S_sorted=S_sorted[["Index","Value"]].reset_index()
            if R_sorted["Index"][i]==S_sorted["Index"][j]:
                #print(R_sorted["Index"][i])
                RjoinS_=[R_sorted["Index"][i],R_sorted["Value"][i],S_sorted["Value"][j]]
                RjoinS.append(RjoinS_)
                print(lines,RjoinS_)        
                S_sorted.drop([[j]])
            lines+=1
            j+=1
            
            #print(len(RjoinS))
            #S_sorted=S_sorted.drop(S_sorted.Index[[j]]).reset_index()
        R_sorted.drop([[i]])
    except KeyError:
        pass
'''


print("The count of lines is: ", lines)
RjoinS=pd.DataFrame(data=RjoinS,columns=("Index","R_Value","S_Value"))
#RjoinS=RjoinS.drop_duplicates(["Index","R_Value","S_Value"])
RjoinS.to_csv(data_alias+"RjoinS"+data_type)
print("The size of the RjoinS is: ",RjoinS.shape)

f=open(data_alias+"log.txt","a")
f.write("--------Assigment 1------------\n")
f.write("The count of lines is:{} \n".format(lines))
f.write("The size of the RjoinS is:{} \n".format(RjoinS.shape))
f.close()

del R_sorted_
del S_sorted_



#Q2  Union (15%)#

RunionS=R_sorted.append(S_sorted).drop_duplicates(["Index","Value"])
RunionS.to_csv(data_alias+"RunionS"+data_type) 
print("The size of the RunionS is: ", RunionS.shape)

f=open (data_alias+"log.txt","a")
f.write("--------Assigment 2------------\n")
f.write("The size of the RunionS is:{} \n".format(RunionS.shape))
f.close()

#Q3 Intersection (15%)#


R_sorted_=R_sorted
S_sorted_=S_sorted

RintersectionS=[]

for idx in (R_sorted_["Index"]):

    try:
        R_=R_sorted_[["Index","Value"]][R_sorted_["Index"]==idx]
        S_=S_sorted_[["Index","Value"]][S_sorted_["Index"]==idx]
        #print(idx,R_)
        R_=R_.sort_values("Value").reset_index()
        S_=S_.sort_values("Value").reset_index()
        n_r=len(R_)
        n_s=len(S_)
        for i in range (n_r):
            for j in range (n_s):
                if R_["Value"][i]==S_["Value"][j]:
                    RintersectionS_=[idx,R_["Value"][i]]
                    print(RintersectionS_)
                    RintersectionS.append(RintersectionS_)
                j+=1
                 
            i+=1
        R_sorted_=R_sorted_[R_sorted_.Index != idx]
        S_sorted_=S_sorted_[S_sorted_.Index != idx]
    except ValueError:
        pass


RintersectionS=pd.DataFrame(data=RintersectionS,columns=("Index","Value"))
RintersectionS=RintersectionS.drop_duplicates(["Index","Value"])
RintersectionS.to_csv(data_alias+"RintersectionS"+data_type)
print("The size of the RintersectionS is: ", RintersectionS.shape)

f=open (data_alias+"log.txt","a")
f.write("--------Assigment 3------------\n")
f.write("The size of the RintersectionS is:{} \n".format(RintersectionS.shape))
f.close()


del R_sorted_
del S_sorted_
              


#Q4 Set difference (15%)#

RdifferenceS=R_sorted.append(RintersectionS).drop_duplicates(keep=False)
RdifferenceS.to_csv(data_alias+"RSdifference"+data_type)
print("The answer of Q4 is: ", RdifferenceS.shape)

f=open (data_alias+"log.txt","a")
f.write("--------Assigment 4------------\n")
f.write("The size of the RdifferenceS is:{} \n".format(RdifferenceS.shape))
f.close()



#Q5 Grouping and Aggregation (30%)

R=pd.read_csv(data_alias+file_name_3+data_type,sep=s,header=None,names=["Index","Value"]).fillna("Not Applicable")

R=R.sort_values(["Index"]).reset_index()

#print("The task is in progress, please wait.")
RgIndex=[]
RgValue=[]


for idx in (R["Index"]):
    print(idx)
    
    R_=R[R["Index"]==idx]
    R_=R_.reset_index()

    n=len(R_)-1
    R_Value=sum(R_["Value"][:])

    RgIndex.append(idx)
    RgValue.append(R_Value)

    
    print(R_Value)
    

RgIndex=np.asarray(RgIndex)
RgValue=np.asarray(RgValue)
Rgroupby=pd.DataFrame({"Index":RgIndex,"Value":RgValue})

Rgroupby=Rgroupby.drop_duplicates(["Index","Value"])
print("The answer of Q5 is: ", Rgroupby.shape)

Rgroupby.to_csv(data_alias+"Rgroupby"+data_type)
print("The task is done.")

f=open (data_alias+"log.txt","a")
f.write("--------Assigment 5------------\n")
f.write("The size of the Rgroupby is:{} \n".format(Rgroupby.shape))
f.close()

        