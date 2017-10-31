# -*- coding: utf-8 -*-
"""
Created on Sat Oct 28 13:33:48 2017

@author: Qi Chen
"""
## This is the script to find most generous political donors. Contribution amounts are grouped
## by (recipient_id, zip code) and (recipient_id, date) respectively. The goal is to get the
## statistics including median, number of transactions and total amount. To maintain the running
## total, number of transactions and the median, we use two hashtables with (recipient_id, zip code)
## and (recipient_id, date) as keys respectively and total_amount, num_transactions, median as values.
## Notice that to calculate the running median each time a new input is given, I maintain a minheap
## to store the larger half and a maxheap to store the smaller half of the past transaction amounts
## under (recipient_id, zip code) and (recipient_id, date) respectively. In this way, one can get
## the median by either poping out from the maxheap or poping out from both heaps to get the average
## depending on whether the number of records is odd or even.

# import all possible dependencies
import sys
import json
import collections
import heapq
import math
import time
import datetime

# I build up a class with different methods to deal with the data stream
class find_political_donors(object):
    def __init__(self):
        # initialize the hashtables for medianvals_by_zip.txt and medianvals_by_date.txt
        self.recipient_zip={}
        self.recipient_date={}
        return
        
    def read_data_stream(self, file_address, output_path):
        """
        input: file_address(str)
        return: nothing
        """
        data_file = open(file_address)
        output_zip_file = open(output_path, 'w')
        for line in data_file:
            # split each record into a list of different fields
            row=line.split('|')
            # extract out relevant fields
            CMTE_ID=row[0]
            ZIP_CODE=row[10][:5]
            TRANSACTION_DT=row[13]
            TRANSACTION_AMT=row[14]
            OTHER_ID=row[15]
            if CMTE_ID and TRANSACTION_AMT and not OTHER_ID: # valid log
                if len(ZIP_CODE)==5: # condition for medianvals_by_zip.txt
                    output_log=self.process_zip(CMTE_ID,ZIP_CODE,TRANSACTION_AMT)
                    output_zip_file.write('|'.join(output_log)+'\n')
                if len(TRANSACTION_DT)==8: # condition for medianvals_by_date.txt
                    self.process_date(CMTE_ID,TRANSACTION_DT,TRANSACTION_AMT)
        data_file.close()
        output_zip_file.close()
                    
    def output_date(self,filepath):
        # output by (recipient_id, date), this is not done simultaneously with the streaming of data
        # as sorting is necessary if the data stream is out of order.
        data_file = open(filepath,'w')
        for CMTE_ID in sorted(self.recipient_date.keys()):
            for date in sorted(self.recipient_date[CMTE_ID]):
                log=[CMTE_ID, date, str(int(self.recipient_date[CMTE_ID][date]['curr_median'])),\
                str(int(self.recipient_date[CMTE_ID][date]['num_trans'])),\
                str(int(self.recipient_date[CMTE_ID][date]['total_amt']))]
                data_file.write('|'.join(log)+'\n')
        data_file.close()
        
    def process_zip(self,CMTE_ID,ZIP_CODE,TRANSACTION_AMT):
        ## This function output the running statistics grouped by (recipient_id, zip)
        ## with the streaming of input data
        """
        input:
        1. CMTE_ID(str): recipient_id
        2. ZIP_CODE(str)
        3. TRANSACTION_AMT(str)
        return: a line in medianvals_by_zip.txt in list form
        """
        if CMTE_ID not in self.recipient_zip:
            self.recipient_zip[CMTE_ID]={}
        if ZIP_CODE not in self.recipient_zip[CMTE_ID]:
            self.recipient_zip[CMTE_ID][ZIP_CODE]\
            ={'num_trans':0.,'total_amt':0.,'max_heap':[],'min_heap':[],'curr_median':None}
        # add to total number of transactions
        self.recipient_zip[CMTE_ID][ZIP_CODE]['num_trans']+=1
        # add to total amount
        self.recipient_zip[CMTE_ID][ZIP_CODE]['total_amt']+=float(TRANSACTION_AMT)
        # calculate curr_median
        self.recipient_zip[CMTE_ID][ZIP_CODE]['curr_median']=\
        self.eval_median(self.recipient_zip[CMTE_ID][ZIP_CODE]['max_heap'],\
        self.recipient_zip[CMTE_ID][ZIP_CODE]['min_heap'],float(TRANSACTION_AMT))
        
        return [CMTE_ID,ZIP_CODE,str(int(self.recipient_zip[CMTE_ID][ZIP_CODE]['curr_median'])),\
        str(int(self.recipient_zip[CMTE_ID][ZIP_CODE]['num_trans'])),\
        str(int(self.recipient_zip[CMTE_ID][ZIP_CODE]['total_amt']))]
        
    def process_date(self,CMTE_ID,TRANSACTION_DT,TRANSACTION_AMT):
        ## This function group the running statistics into (recipient_id, date)
        ## but does not output with the streaming of input data
        """
        input:
        1. CMTE_ID(str): recipient_id
        2. TRANSACTION_DT(str)
        3. TRANSACTION_AMT(str)
        return: nothing
        """
        if CMTE_ID not in self.recipient_date:
            self.recipient_date[CMTE_ID]={}
        if TRANSACTION_DT not in self.recipient_date[CMTE_ID]:
            self.recipient_date[CMTE_ID][TRANSACTION_DT]\
            ={'num_trans':0.,'total_amt':0.,'max_heap':[],'min_heap':[],'curr_median':None}
        # add to total number of transactions
        self.recipient_date[CMTE_ID][TRANSACTION_DT]['num_trans']+=1
        # add to total amount
        self.recipient_date[CMTE_ID][TRANSACTION_DT]['total_amt']+=float(TRANSACTION_AMT)
        # calculate curr_median
        self.recipient_date[CMTE_ID][TRANSACTION_DT]['curr_median']=\
        self.eval_median(self.recipient_date[CMTE_ID][TRANSACTION_DT]['max_heap'],\
        self.recipient_date[CMTE_ID][TRANSACTION_DT]['min_heap'],float(TRANSACTION_AMT))
        
        
    def eval_median(self, max_heap, min_heap, new_amount):
        ## To get the median, the most efficient way I can come up with is by keeing
        ## a maxheap of the n/2 smaller contributions, and the n/2 larger contributions
        ## Notice that in this implementation size_maxheap-size_minheap==0 if n even else
        ## size_maxheap-size_minheap==1
        """
        input:
        1.max_heap(heap): a max heap to store the lower half of the transaction amounts
        2.min_heap(heap): a min heap to store the upper half of the transaction amounts
        3.new_amount(float): newly arrival transaction amount
        return:
        current median(float)
        """
        if max_heap==[] or -max_heap[0]>new_amount:
            heapq.heappush(max_heap,-new_amount)
            if len(max_heap)-len(min_heap)>1:
                temp=-heapq.heappop(max_heap)
                heapq.heappush(min_heap, temp)
        else:
            heapq.heappush(min_heap, new_amount)
            if len(min_heap)-len(max_heap)>0:
                temp=heapq.heappop(min_heap)
                heapq.heappush(max_heap, -temp)
        
        if len(max_heap)>len(min_heap):
            return round(-max_heap[0],0)
        else:
            return round((min_heap[0]-max_heap[0])/2.,0)
# create an instance of solution
solution=find_political_donors()
# read from system input
input_path=sys.argv[1]
output_zip_path=sys.argv[2]
output_date_path=sys.argv[3]
# data processing and output
solution.read_data_stream(input_path, output_zip_path)        
solution.output_date(output_date_path)    
    

