import subprocess
import json
import csv
import os
import threading
import time
from decimal import Decimal
from error import *
from settings import *

class HiveConnector():
    def __init__(self) -> None:
        self.hiveurl = None
        self.hiveport = None
        self.hiveacc = None
        self.hivepwd = None

        
class GetBlockAndTxInfo:
    def __init__(self, temp_code, start_block, end_block, step) -> None:
        # javascript code for query in geth console
        self.javascript_code = r"""
                                const contractABI = [];
                                const contractAddress = '';
                                const contract = eth.contract(contractABI).at(contractAddress);
                                const blockNumber = <<BLOCKNUMBER>>;
                                const blockData = eth.getBlock(blockNumber, true);
                                const cleanedBlockData = {};
                                for (const key in blockData) {
                                  if (blockData[key] !== null) {
                                    cleanedBlockData[key] = blockData[key];
                                  }
                                }
                                const data = {
                                 cleanedBlockData,
                                };
                                const jsonData = JSON.stringify(data, null ,2);
                                console.log(jsonData);
                                """
        self.temp_filepath = f'temp/temp{temp_code}.js'
        # this parameter should be modified while this project is implemented
        self.temp_longfilepath = f"/home/rivending/test/temp/temp{temp_code}.js"
        # change the key data that in need
        self.block_basic_info_selected_keys = ['difficulty','gasLimit','gasUsed','hash','miner','mixHash','nonce','number','parentHash','receiptsRoot','sha3Uncles','size','stateRoot','timestamp','totalDifficulty','transactionsRoot']
        self.tx_info_selected_keys = ["blockHash","blockNumber","from","gas","gasPrice","maxFeePerGas","maxPriorityFeePerGas","hash","nonce","to","transactionIndex","value","type","chainId","v","r","s","yParity"]
        self.start_block = start_block
        self.end_block = end_block
        self.step = step

    # run the command by automatically python script
    def run_command_with_changed_parameter(self, javascript_code, parameter_value):
        modified_code = javascript_code.replace('<<BLOCKNUMBER>>', str(parameter_value))
        ############ here should be modified while useing the multithread, file name should be unique! ############
        temp_filename = self.temp_filepath
        with open(temp_filename, 'w') as f:
            f.write(modified_code)
        # build the correct command
        command_inside = f'loadScript("{self.temp_longfilepath}")'
        command = f"geth --exec '{command_inside}' attach"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        output = result.stdout.strip()
        # execute the tmp file on the terminal
        try:
            subprocess.run(f'rm {temp_filename}', shell=True)
        except:
            print("error occurred")
            pass
        # the output on the terminal is a string type before transferring to json type 
        result_str = self.remove_null_suffix(output)
        # if something wrong happens, it cannot be formated as a json type
        try:
            result_json = json.loads(result_str)
            block_data = result_json["cleanedBlockData"]
            return block_data
        except SyntaxError:
            return Error_1

    # remove null
    def remove_null_suffix(self, string) -> str:
        if string.endswith("null"):
            string = string[:-4]
        else:
            pass
        return string

    # format dict func, for the purpose
    def store_selected_keys(self, dictionary, selected_keys) -> dict:
        data_dict = {key: value for key, value in dictionary.items() if key in selected_keys}
        for tobe_changed_key in ['gasPrice','maxFeePerGas','maxPriorityFeePerGas','value']:
            if tobe_changed_key in data_dict:
                try:
                    value = int(data_dict[tobe_changed_key])
                # ERROR SAMPLE: ValueError: invalid literal for int() with base 10: '1.97522534544e+21'
                except ValueError:
                    value = Decimal(str(value))              
                data_dict[tobe_changed_key] = self.tx_value_calculator(value)
            else:
                continue
        return data_dict

    # return dict, additionanlly, if data should be written into a csv, headers should be a parameter to return
    def get_block_basic_info_dict(self, block_data) -> dict:
        block_basic_info_selected_keys = self.block_basic_info_selected_keys
        basic_info_dict = self.store_selected_keys(block_data, block_basic_info_selected_keys)
        # write the dictionary into database or csv
        return block_basic_info_selected_keys, basic_info_dict

    # return list[dict], additionanlly, if data should be written into a csv, headers should be a parameter to return
    def get_tx_dict(self, txs_list) -> list:
        tx_info_selected_keys = self.tx_info_selected_keys
        # this dict will contain all the transactions info in one block before writing into the database
        tx_dict_list = []
        for txs in txs_list:
            tx_dict = self.store_selected_keys(txs, tx_info_selected_keys)
            tx_dict_list.append(tx_dict)
        # write a dictionary list into database or csv
        return tx_info_selected_keys, tx_dict_list

    # use csv for a data demo
    def write_dict_to_csv(self, data, file_path, column_names):
        file_exists = os.path.isfile(file_path)
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
        return None

    # calculate the transaction value from str to int, from wei to eth
    def tx_value_calculator(self, wei_amount):
        eth_amount = wei_amount / 10**18
        value = eth_amount
        return value

    # if sample, use csv for storage while database, change the related code
    def run(self):
        for parameter_value in range(self.start_block,self.end_block,self.step):
            # print(f'Running the block, block number is {parameter_value}', end = '\r')
            block_data = self.run_command_with_changed_parameter(self.javascript_code, parameter_value)
            # if geth console can't execute javascript code in succeed
            if block_data == Error_1:
                # record the error block
                with open('error_block.txt', 'a', newline='') as f:
                    f.write(str(parameter_value)+',')
                continue
            # execute in succeed !
            else:
                # block basic info
                block_basic_info_selected_keys, block_info_dict = self.get_block_basic_info_dict(block_data)
                basic_info_list = [block_info_dict]
                # write into a csv file 
                self.write_dict_to_csv(basic_info_list, 'blockinfo_sample.csv', block_basic_info_selected_keys)
                # transaction list
                txs_list = block_data["transactions"]
                tx_info_selected_keys, tx_dict_list = self.get_tx_dict(txs_list)
                # make a record for verification if there is no tx info in one block
                if len(tx_dict_list) == 0:
                    with open('no_tx_block.txt', 'a', newline='') as f:
                        f.write(str(parameter_value)+',')
                else:
                    pass
                self.write_dict_to_csv(tx_dict_list, 'txinfo_sample.csv', tx_info_selected_keys)     
        return None


class MultiWorker:
    # batch_capacity means, how many blocks in one batch
    def __init__(self, nprocess, batch_capacity) -> None:
        self.nprocess = nprocess
        self.batch_capacity = batch_capacity
        self.new_round_start_block = 15666000
        return None
    
    def multithread(self) -> None:
        # thread number
        nprocess = self.nprocess
        # each thread execute 100 blocks' data
        batch_capacity = self.batch_capacity
        # every batch would execute many times
        new_round_start_block = self.new_round_start_block
        # j is depended on the amount of the batch 19000000/(batch_capacity*nprocess)+1 and should be an int
        try:
            circle_round = int(1000/(batch_capacity*nprocess) + 1)
        except:
            print("Something wrong! Please set the correct circle times and make sure it's an int!")
            return None
        for j in range(1,circle_round):
            threads = []
            # asign temp file number
            for i in range(0, nprocess): # 10*threads
                start_block = i*batch_capacity + new_round_start_block
                end_block = start_block + batch_capacity
                # asign each start block and end block in each thread
                thread = threading.Thread(target=for_thread_running, args=(i, start_block, end_block, 1))
                threads.append(thread)
                print(f'Round:{j}|===>|Thread:{i} has been prepared, start block is {start_block} !')
            # start all the threads
            for thread in threads:
                thread.start()
            # wait for all the threads been prepared
            for thread in threads:
                thread.join()
            new_round_start_block = new_round_start_block + i*batch_capacity
        print("FINISHED ALL TASKS IN THIS ROUND! HAVE A NICE DAY!")
        return None


def for_thread_running(temp_code, start_block, end_block, step) -> None:
    # parameter format (temp file number, start block number, end block number)
    getinfo = GetBlockAndTxInfo(temp_code, start_block, end_block, step)
    getinfo.run()
    return None

# test running time 
start_time = time.time()

if __name__ == "__main__":
    # MultiWorker parameters : nprocess, batch_capacity
    worker = MultiWorker(10, 100)
    worker.multithread()


# calculate time
end_time = time.time()
elapsed_time = end_time - start_time
print("running for {:.2f} seconds".format(elapsed_time))