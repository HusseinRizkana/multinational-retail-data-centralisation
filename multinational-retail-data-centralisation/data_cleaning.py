import pandas as pd
import datetime
import pycountry
import re
class DataCleaning:
    


    def duplicate_remove(self,df:pd.DataFrame):
        duplicates = df.duplicated(subset=['first_name',	'last_name',	'date_of_birth'])
        df[duplicates]
        return df[~duplicates]

    def contact_validate(self,s:str):
        try: 
            s = s.replace("(","")
            s = s.replace(")","")
            s = s.replace(" ", "")

            int(s)
            return True
        except ValueError:
            return False
        
    # date formats
    def date_format(self,x:str):
        try :
            if len(x.split("-"))==3 and x.replace("-","").isnumeric():
                return x
            if " " in x:
                x = x.split(" ")
                for i in x: 
                    if i.isalpha():
                        month = datetime.datetime.strptime(i, '%B').month
                    elif len(i)<3:
                        day = i
                    else: year = i
                return f"{year}-{month}-{day}"
            elif x[4] == "/" and x[6]=="/":
                return x.replace("/","-")
            else:return None
        except:
            return None




        
        
    def phone_clean(self,df:pd.DataFrame):
        # deal with phone numbers
        df["phone_number"]= df["phone_number"].apply(lambda x:str(x))
        df["phone_number"]=df["phone_number"].loc[df['phone_number'].apply(self.contact_validate)]
         
        return df


    # deal with null values
    def null_to_none(self,x):
        return x if x != "NULL" else None

    def remove_null(self,df:pd.DataFrame):
        df = df.applymap(self.null_to_none)
        df.dropna(inplace = True)
        return df



    def get_country_name(self,code):
        try:
            country = pycountry.countries.get(alpha_2=code.upper())
            return country.name.capitalize()
        except AttributeError:
            return None


    def get_country_code(self,name):
        try:
            country = pycountry.countries.search_fuzzy(name)[0]
            return country.alpha_2
        except LookupError:
            return None


    def fill_none_countries(self,df:pd.DataFrame):
        for i in range(len(df["country"])):
            if df["country"][i] !=None and df["country_code"][i] ==None:
                df["country_code"][i] = self.get_country_code(df["country"][i])
            elif df["country"][i] ==None and df["country_code"][i] !=None:
                df["country"][i] = self.get_country_name(df["country_code"][i])
            elif df["country"][i] ==None and df["country_code"][i]==None:
                pass
        return df
    

    

    def dob_clean(self,df:pd.DataFrame):
        #format dob as datetime
        df["date_of_birth"] = df["date_of_birth"].apply(self.date_format)
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"])
        return df

    def join_date_clean(self,df):
        #format join date as datetime
        df["join_date"] = df["join_date"].apply(self.date_format)
        df["join_date"] = pd.to_datetime(df["join_date"])
        return df

    def first_name_clean(self,df:pd.DataFrame):
        df["first_name"] = df["first_name"].loc[df["first_name"].str.isalpha()]
        df["first_name"] = df["first_name"].str.lower()
        df["first_name"] = df["first_name"].str.capitalize()

        return df
    def last_name_clean(self,df:pd.DataFrame):
        df["last_name"] = df["last_name"].loc[df["last_name"].str.isalpha()]
        df["last_name"] = df["last_name"].str.lower()
        df["last_name"] = df["last_name"].str.capitalize()
        return df
    
    def company_clean(self,df:pd.DataFrame):
        return df
    
    def email_address_clean(self,df:pd.DataFrame):
        df["email_address"] = df["email_address"].astype(str)
        # Create a boolean mask of email addresses containing both "@" and "."
        mask = df["email_address"].str.contains("@") & df["email_address"].str.contains(".")

        # Create a new DataFrame with filtered email addresses
        df["email_address"] = df["email_address"].loc[mask]

        df["email_address"] = df["email_address"].str.lower()
        return df
    
    def address_clean(self,df:pd.DataFrame):
        df["address"] = df["address"].loc[df["address"] != "null"]
        df["address"] = df["address"].loc[df["address"] != ""]
        return df
    def check_str(self,string):
        return True if all(x.isalpha() or x.isspace() for x in string) else False
    def country_clean(self,df:pd.DataFrame):
        df["country"] = df["country"].loc[df["country"].apply(self.check_str)]
        df["country"] = df["country"].str.capitalize()
        return df
    def user_uuid_clean(self,df:pd.DataFrame):
        df["user_uuid"] = df["user_uuid"].loc[df["user_uuid"].str.len()==36]
        return df
    def dropnull(self,df:pd.DataFrame):
        return  df.dropna(subset=["first_name","last_name"])
        
    def clean_duplicates(self,df):
        df = self.duplicate_remove(df)
        return df
    
    def clean_None(self,df:pd.DataFrame):
        df = self.fill_none_countries(df)
        df = df.applymap(self.null_to_none)
        return df
    
    def clean_columns(self,df:pd.DataFrame):
        df = self.user_uuid_clean(df)
        df = self.dob_clean(df)
        df = self.join_date_clean(df)
        df = self.phone_clean(df)
        df = self.address_clean(df)
        df= self.company_clean(df)
        df = self.email_address_clean(df)
        df = self.first_name_clean(df)
        df = self.last_name_clean(df)
        df = self.country_clean(df)   
        df = self.clean_None(df) 
        df = self.clean_duplicates(df)
        df = self.dropnull(df)
        return df
   