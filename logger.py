import os 
import datetime


class logger:
    def __init__(self, fileName= os.path.join(os.path.abspath("logs"), "laststreamingdate.txt") ):
        self.fileName = fileName
        with open(self.fileName, "a") as file:
            file.write("")
            
            
    def info(self, message):
        
        self.__write_message__(f"[info]\t{message}")
        
        
    
    def __write_message__(self, message):
        
        current_date = datetime.datetime.now() 
        
        with open(self.fileName, "r+") as file:
            file.write(f"{current_date.strftime('%Y%C%d %H:%M:%S')} ")