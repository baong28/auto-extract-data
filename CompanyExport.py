from logging import warning
from asyncio.log import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import xlsxwriter
import time, os, re, glob
import pandas as pd
import itertools
import clipboard
from xlsx2csv import Xlsx2csv
from joblib import Parallel, delayed
from get_logger import info_n_telegram_sendtext, warning_n_telegram_sendtext, error_n_telegram_sendtext
from datetime import timedelta as dt
import datetime as dt1
from pandas.tseries.offsets import MonthEnd
from logging.handlers import TimedRotatingFileHandler

n_jobs=1

class CompanyExport:
    '''
    companyExport Class containing various export functions and pipelines

    '''
    def __init__(self, company_conn, company_website, company_username, company_password, company_landing_area_dir, export_type, from_date, to_date, output_path, csv_path, usernameTxtBox, passwordTxtBox, submitButton, archive_path, xlsx_kwargs, to_csv_kwargs, logger, roomID ):
        # Shared variables
        self.company_conn = company_conn
        self.company_website = company_website
        self.company_username = company_username
        self.company_password = company_password
        self.company_landing_area_dir = company_landing_area_dir

        self.output_path = output_path
        self.csv_path = csv_path
        self.archive_path = archive_path

        self.usernameTxtBox = usernameTxtBox
        self.passwordTxtBox = passwordTxtBox
        self.submitButton = submitButton

        self.xlsx_kwargs = xlsx_kwargs
        self.to_csv_kwargs = to_csv_kwargs
        self.logger = logger
        self.roomID = roomID
        
        self.from_date = from_date
        self.to_date = to_date
        self.export_type = export_type
        self.cur_date = pd.to_datetime("today")

    def connection ( self ):
        '''
        Lauching Company Admin and connect to Webdriver
        '''
        chromes_options = webdriver.ChromeOptions()
        prefs = {"profile.default_content_settings.popups": 0,
            "download.default_directory": r".\company-group\landing_area", # IMPORTANT - ENDING SLASH V IMPORTANT
            "directory_upgrade": True}
        chromes_options.add_experimental_option("prefs", prefs)
        chromes_options.add_argument('--headless')
        chromes_options.add_argument('--disable-gpu')
        chromes_options.add_argument('--no-sandbox')
        chromes_options.add_argument('--disable-dev-shm-usage')
        chromes_options.add_argument('--dns-prefetch-disable')
        chromes_options.add_argument('--disable-extensions')

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chromes_options)
        driver.set_page_load_timeout(15)
        driver.set_script_timeout(15)
        driver.get(self.company_website)
        driver.maximize_window()
        time.sleep(10)
        return driver

    def login ( self, driver ):
      # Log into account
        driver.find_element(By.XPATH, self.usernameTxtBox).send_keys(self.company_username)
        driver.find_element(By.XPATH, self.passwordTxtBox).send_keys(self.company_password)
        driver.find_element(By.XPATH, self.submitButton).click()
        time.sleep(10)
    
    def login_company( self ):
        '''
        Login to Company Admin pipeline
        '''
        try:
            driver = self.connection()
            self.login(driver)
            time.sleep(5)
            return driver
        
        except Exception as e:
            error_n_telegram_sendtext( self.logger, f'{self.export_type} || login_company_website || Encounter error: {str(e)}', self.roomID )
            raise

    def close_company( self, driver ):
        '''
        Close company webdriver
        '''
        # Close company AFTER looping
        try:
            self.logger.info(f'{self.export_type} || close_company || Closing company website')
            time.sleep(30)
            driver.quit()

        except Exception as e:
            error_n_telegram_sendtext( self.logger, f'{self.export_type} || close_company || Encounter error: {str(e)}', self.roomID )
            raise

    # Define to-csv convert pipeline functions     
    def xlsx2csv_convert( self, xlsx_file, csv_out_file ):
        # Convert .xlsx to .csv
        Xlsx2csv(f'{self.output_path}\\{xlsx_file}').convert(f'{self.csv_path}\\{csv_out_file}')
        # Move converted .xlsx to ./archive/
        os.replace(f'{self.output_path}\\{xlsx_file}', f'{self.archive_path}\\{xlsx_file}')
        
    # Define non_xlsx2csv convert pipeline functions     
    def non_xlsx2csv_convert(self, non_xlsx_file, csv_out_file, read_csv_kwargs, to_csv_kwargs):
        # Read non-xlsx input file
        df = pd.read_csv(f'{self.output_path}\\{non_xlsx_file}', **read_csv_kwargs)
        # Remove blank spaces in column names
        df.columns = df.columns.str.strip()
        # Reset row index
        df.reset_index(drop=True, inplace=True)
        # Replace in-column special character ',' '$', ' ' with empty space ''
        df.replace('[,$ ]+', '', regex=True, inplace=True)
        # Drop columns with all NA Values
        df.dropna(axis=1, how='all', inplace=True)
        # Move converted NON-.xlsx to ./archived/
        os.replace(f'{self.output_path}\\{non_xlsx_file}', f'{self.archive_path}\\{non_xlsx_file}')
        # Write non-xlsx to csv file
        df.to_csv(f'{self.csv_path}\\{csv_out_file}', **to_csv_kwargs)
        
    def to_csv_convert_pipeline( self, read_csv_kwargs=None, to_csv_kwargs=None):
        info_n_telegram_sendtext( self.logger, f'{self.export_type} || to_csv_convert_pipeline || Applying to-csv convert pipeline', self.roomID)
        
        # Kill EXCEL.exe processes
        os.system("taskkill /F /im EXCEL.EXE")

        # Define (NON) xlsx and csv target file lists
        xlsx_file_list =  [f for f in os.listdir(self.archive_path) if any(f.endswith(s) for s in [".xlsx",".xls"])]
        # xlsx_file_list = [ f for f in os.listdir(self.archive_path) if re.search(r"[.](?:xlsx || xls)$", f) is not None ]

        non_xlsx_file_list = [f for f in os.listdir(self.archive_path) if any(f.endswith(s) for s in [".txt",".tsv"])]
        # non_xlsx_file_list = [ f for f in os.listdir(self.archive_path) if re.search(r"[.](?:txt || tsv)$", f) is not None ]
        
        try:
            if ( len( xlsx_file_list ) > 0 ):
                csv_out_file_list = [ f"{f.split('.xlsx')[0]}.csv" for f in xlsx_file_list ]
                
                # Convert .xlsx to .csv
                Parallel( n_jobs=n_jobs )( delayed( self.xlsx2csv_convert )( xlsx_file_list[i], csv_out_file_list[i] ) for i in range( len( xlsx_file_list ) ) )
                
            elif ( len( non_xlsx_file_list ) > 0 ):
                csv_out_file_list = [ f"{'.'.join(f.split('.')[:-1])}.csv" for f in non_xlsx_file_list ]
                
                # Convert NON .xlsx to .csv
                Parallel( n_jobs=n_jobs )( delayed( self.non_xlsx2csv_convert )( non_xlsx_file_list[i], csv_out_file_list[i], read_csv_kwargs, to_csv_kwargs ) for i in range( len( non_xlsx_file_list ) ) )

        except Exception as e:
            error_n_telegram_sendtext( self.logger, f'{self.export_type} || to_csv_convert_pipeline || Encounter error: {str(e)}', self.roomID )
            raise

    def remove_files(self, names):        
        ## remove file .xlsx have downloaded
        for f in glob.iglob(f'{self.company_landing_area_dir}\\landing_area'+'/**/*.xlsx', recursive=True):
            if any(f.endswith(s) for s in names) is True:
               os.remove(f)

    def Transform_Company_Revenue(self):
        items = os.listdir(f'{self.company_landing_area_dir}\\landing_area')
        today = dt1.datetime.strftime(dt1.date.today(),"%d%m%Y")
        names = [names for names in items if names.endswith(f"-DoanhThu-{today}.xlsx") is True]
        month_start = pd.date_range(self.from_date, self.to_date, freq='MS').strftime('%d.%m.%Y').tolist()
        month_end = pd.date_range(self.from_date, self.to_date, freq='ME').strftime('%d.%m.%Y').tolist()
        date_range = pd.DataFrame({"month_start": month_start,"month_end": month_end})
        month_start_end = (date_range['month_start']+'_'+date_range['month_end'])
        rename = 'Company_revenue_'+ month_start_end + '.xlsx'
    
        ## read downloaded files
        df = pd.read_excel(f'{self.company_landing_area_dir}\\landing_area\\{names[0]}', skipfooter=2, skiprows=1)

        ## load files to new directory
        writer = pd.ExcelWriter(f'{self.company_landing_area_dir}\\raw_data\\{rename[0]}', engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer._save()
        writer.close()

        ## Remove just downloaded files
        self.remove_files(names)

    def Transform_Company_Regimen(self):
        items = os.listdir(f'{self.company_landing_area_dir}\\landing_area')
        today = dt1.datetime.strftime(dt1.date.today(),"%d%m%Y")
        names = [names for names in items if names.endswith(f"lieu trinh - {today}.xlsx") is True]
        month_start = pd.date_range(self.from_date, self.to_date, freq='MS').strftime('%d.%m.%Y').tolist()
        month_end = pd.date_range(self.from_date, self.to_date, freq='ME').strftime('%d.%m.%Y').tolist()
        date_range = pd.DataFrame({"month_start": month_start,"month_end": month_end})
        month_start_end = (date_range['month_start']+'_'+date_range['month_end'])
        rename = 'Company_regimen_'+ month_start_end + '.xlsx'

        ## read downloaded files
        df = pd.read_excel(f'{self.company_landing_area_dir}\\landing_area\\{names[0]}', skipfooter=2, skiprows=1)

        ## load files to new directory
        writer = pd.ExcelWriter(f'{self.company_landing_area_dir}\\raw_data\\{rename[0]}', engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer._save()
        writer.close()

        ## Remove just downloaded files
        self.remove_files(names)

    def Transform_Company_Services(self):
        items = os.listdir(f'{self.company_landing_area_dir}\\landing_area')
        today = dt1.datetime.strftime(dt1.date.today(),"%d%m%Y")
        names = [names for names in items if names.endswith(f"dich vu - {today}.xlsx") is True]
        month_start = pd.date_range(self.from_date, self.to_date, freq='MS').strftime('%d.%m.%Y').tolist()
        month_end = pd.date_range(self.from_date, self.to_date, freq='ME').strftime('%d.%m.%Y').tolist()
        date_range = pd.DataFrame({"month_start": month_start,"month_end": month_end})
        month_start_end = (date_range['month_start']+'_'+date_range['month_end'])
        rename = 'Company_services_'+ month_start_end + '.xlsx'
    
        ## read downloaded files
        df = pd.read_excel(f'{self.company_landing_area_dir}\\landing_area\\{names[0]}', skipfooter=2, skiprows=1)

        ## load files to new directory
        writer = pd.ExcelWriter(f'{self.company_landing_area_dir}\\raw_data\\{rename[0]}', engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer._save()
        writer.close()

        ## Remove just downloaded files
        self.remove_files(names)

    def Extract_Company_Revenue( self, driver, from_date, to_date ):
        '''
        Export company Revenue 

        '''
        filename = from_date + '_' + to_date + "_Company.xlsx"
        try:
            driver.find_element(By.XPATH, "//a[@href='/bao-cao']").click()
            time.sleep(10)
            driver.find_element(By.XPATH, "//a[@id='bao-cao-doanh-thu-btn']").click()
            time.sleep(10)
            driver.find_element(By.XPATH, "//span[@id='select2-dateFilter-container']").click()
            time.sleep(10)
            driver.find_element(By.XPATH, "//span[@class='select2-dropdown select2-dropdown--below']/span[2]/ul/li[7]").click()
            time.sleep(10)
            driver.find_element(By.XPATH, "//input[@id='from']").click()
            driver.find_element(By.XPATH, "//input[@id='from']").clear()
            time.sleep(5)
            driver.find_element(By.XPATH, "//input[@id='from']").send_keys(from_date)
            time.sleep(5)
            driver.find_element(By.XPATH, "//input[@id='to']").click()
            driver.find_element(By.XPATH, "//input[@id='to']").clear()
            time.sleep(5)
            driver.find_element(By.XPATH, "//input[@id='to']").send_keys(to_date)
            time.sleep(5)
            driver.find_element(By.XPATH, "//button[@id='search-button']").click()
            time.sleep(10)
            driver.find_element(By.XPATH, "//button[@id='export-excel-orange']").send_keys(Keys.ENTER)
            os.system("taskkill /F /im EXCEL.EXE")
        except:
            error_n_telegram_sendtext( self.logger, f"{self.export_type} || export_{self.export_type} || {self.to_date} || Failed at {filename}", self.roomID )
        
        time.sleep(60)

        return filename
    
    def Company_Revenue_Extract_Pipelines( self ):
        # Initialize session and Log into SAP Transaction system
        info_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline || Logging into company Admin Website', self.roomID )
        driver = self.login_company()

        # Define target date range
        month_start = pd.date_range(self.from_date, self.to_date, freq='MS').strftime('%d/%m/%Y').tolist()
        month_end = pd.date_range(self.from_date, self.to_date, freq='ME').strftime('%d/%m/%Y').tolist()
        date_range = pd.DataFrame({"month_start": month_start,"month_end": month_end})
        month_start_end = (date_range['month_start']+'_'+date_range['month_end'])
        expand_grid_df = pd.DataFrame( itertools.product( month_start_end ), columns=['month_start_end'] )

        # Split and drop some columns
        expand_grid_df[ ['month_start','month_end'] ] = expand_grid_df['month_start_end'].str.split('_', expand=True)
        expand_grid_df.drop( columns=['month_start_end'], inplace=True )
        
        if expand_grid_df.shape[0] > 0:
            # For loop
            for i in range( expand_grid_df.shape[0] ):
                temp_target = expand_grid_df.iloc[i:i+1, :].to_dict( orient='records' )[0]
                temp_from_date = temp_target.get('month_start')
                temp_to_date = temp_target.get('month_end')
                try:
                    info_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline_{temp_from_date}_{temp_to_date} || Exporting layout {temp_from_date}_{temp_to_date}', self.roomID )
                    self.Extract_Company_Revenue( 
                        driver
                        , temp_from_date
                        , temp_to_date
                    )
                    os.system("taskkill /F /im EXCEL.EXE")
                except Exception as e:
                    error_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline_{temp_from_date}_{temp_to_date} || Encounter error: {str(e)}', self.roomID )
                    continue
            
            # Close company AFTER looping
            self.close_company( driver )

             # Transform and change directory 
            self.Transform_Company_Revenue()       

        else:
            warning_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline || Warning! NO target to process company export', self.roomID )
            # Close company AFTER looping
            self.close_company( driver )

    def Extract_Company_Regimen(self, driver, from_date, to_date):
            '''
            Export company Regimen

            '''
            filename = from_date + '_' + to_date + "_Company.xlsx"
            try:
                driver.find_element(By.XPATH, "//a[@href='/bao-cao']").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//a[@id='bao-cao-nhom-lieu-trinh']").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//span[@id='select2-dateFilter-container']").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//span[@class='select2-dropdown select2-dropdown--below']/span[2]/ul/li[7]").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//input[@id='from']").click()
                driver.find_element(By.XPATH, "//input[@id='from']").clear()
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='from']").send_keys(from_date)
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='to']").click()
                driver.find_element(By.XPATH, "//input[@id='to']").clear()
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='to']").send_keys(to_date)
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='to']").send_keys(Keys.ENTER)
                time.sleep(5)
                driver.find_element(By.XPATH, "//button[@class='btn btn-primary pull-left createInput export-excel-button']").click()
                os.system("taskkill /F /im EXCEL.EXE")
            except:
                error_n_telegram_sendtext( self.logger, f"{self.export_type} || export_{self.export_type} || {self.to_date} || Failed at {filename}", self.roomID )
            
            time.sleep(60)

            return filename
        
    def Company_Regimen_Extract_Pipelines( self ):
        # Initialize session and Log into SAP Transaction system
        info_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline || Logging into company Admin Website', self.roomID )
        driver = self.login_company()

        # Define target date range
        month_start = pd.date_range(self.from_date, self.to_date, freq='MS').strftime('%d/%m/%Y').tolist()
        month_end = pd.date_range(self.from_date, self.to_date, freq='ME').strftime('%d/%m/%Y').tolist()
        date_range = pd.DataFrame({"month_start": month_start,"month_end": month_end})
        month_start_end = (date_range['month_start']+'_'+date_range['month_end']).tolist()
        expand_grid_df = pd.DataFrame( itertools.product( month_start_end ), columns=['month_start_end'] )

        # Split and drop some columns
        expand_grid_df[ ['month_start','month_end'] ] = expand_grid_df['month_start_end'].str.split('_', expand=True)
        expand_grid_df.drop( columns=['month_start_end'], inplace=True )
        
        if expand_grid_df.shape[0] > 0:
            # For loop
            for i in range( expand_grid_df.shape[0] ):
                temp_target = expand_grid_df.iloc[i:i+1, :].to_dict( orient='records' )[0]
                temp_from_date = temp_target.get('month_start')
                temp_to_date = temp_target.get('month_end')
                try:
                    info_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline_{temp_from_date}_{temp_to_date} || Exporting layout {temp_from_date}_{temp_to_date}', self.roomID )
                    self.Extract_Company_Regimen( 
                        driver
                        , temp_from_date
                        , temp_to_date
                    )
                    os.system("taskkill /F /im EXCEL.EXE")
                except Exception as e:
                    error_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline_{temp_from_date}_{temp_to_date} || Encounter error: {str(e)}', self.roomID )
                    continue
            
            # Close company AFTER looping
            self.close_company( driver )

            # Transform and change directory 
            self.Transform_Company_Regimen()            
        else:
            warning_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline || Warning! NO target to process company export', self.roomID )
            # Close company AFTER looping
            self.close_company( driver )

    def Extract_Company_Service(self, driver, from_date, to_date):
            '''
            Export Company Service 

            '''
            filename = from_date + '_' + to_date + "_Company.xlsx"
            try:
                driver.find_element(By.XPATH, "//a[@href='/bao-cao']").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//a[@id='bao-cao-nhom-dich-vu']").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//span[@id='select2-dateFilter-container']").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//span[@class='select2-dropdown select2-dropdown--below']/span[2]/ul/li[7]").click()
                time.sleep(10)
                driver.find_element(By.XPATH, "//input[@id='from']").click()
                driver.find_element(By.XPATH, "//input[@id='from']").clear()
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='from']").send_keys(from_date)
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='to']").click()
                driver.find_element(By.XPATH, "//input[@id='to']").clear()
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='to']").send_keys(to_date)
                time.sleep(5)
                driver.find_element(By.XPATH, "//input[@id='to']").send_keys(Keys.ENTER)
                time.sleep(10)
                driver.find_element(By.XPATH, "//button[@class='btn btn-primary pull-left createInput export-excel-button']").click()
                os.system("taskkill /F /im EXCEL.EXE")
            except:
                error_n_telegram_sendtext( self.logger, f"{self.export_type} || export_{self.export_type} || {self.to_date} || Failed at {filename}", self.roomID )
            
            time.sleep(60)

            return filename

    def Company_Service_Extract_Pipelines( self ):
        # Initialize session and Log into SAP Transaction system
        info_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline || Logging into company Admin Website', self.roomID )
        driver = self.login_company()

        # Define target date range
        month_start = pd.date_range(self.from_date, self.to_date, freq='MS').strftime('%d/%m/%Y').tolist()
        month_end = pd.date_range(self.from_date, self.to_date, freq='ME').strftime('%d/%m/%Y').tolist()
        date_range = pd.DataFrame({"month_start": month_start,"month_end": month_end})
        month_start_end = (date_range['month_start']+'_'+date_range['month_end']).tolist()
        expand_grid_df = pd.DataFrame( itertools.product( month_start_end ), columns=['month_start_end'] )

        # Split and drop some columns
        expand_grid_df[ ['month_start','month_end'] ] = expand_grid_df['month_start_end'].str.split('_', expand=True)
        expand_grid_df.drop( columns=['month_start_end'], inplace=True )
        
        if expand_grid_df.shape[0] > 0:
            # For loop
            for i in range( expand_grid_df.shape[0] ):
                temp_target = expand_grid_df.iloc[i:i+1, :].to_dict( orient='records' )[0]
                temp_from_date = temp_target.get('month_start')
                temp_to_date = temp_target.get('month_end')
                try:
                    info_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline_{temp_from_date}_{temp_to_date} || Exporting layout {temp_from_date}_{temp_to_date}', self.roomID )
                    self.Extract_Company_Service( 
                        driver
                        , temp_from_date
                        , temp_to_date
                    )
                    os.system("taskkill /F /im EXCEL.EXE")
                except Exception as e:
                    error_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline_{temp_from_date}_{temp_to_date} || Encounter error: {str(e)}', self.roomID )
                    continue
            
            # Close company AFTER looping
            self.close_company( driver )
            
            # Transform and change directory 
            self.Transform_Company_Services()
            
        else:
            warning_n_telegram_sendtext( self.logger, f'{self.export_type} || export_{self.export_type}_pipeline || Warning! NO target to process company export', self.roomID )
            # Close company AFTER looping
            self.close_company( driver )
   


        
